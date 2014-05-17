/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.FrameMemManager;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.GrouperFlushOption;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IGrouperFlushOption;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IGrouperFlushOption.GroupOutputState;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalCommonCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalHashCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalSortCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.RequiredCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashTableFrameTupleAppender;

/**
 * A hybrid-hash grouper groups records so that an in-memory hash table is built
 * to complete the aggregation
 * for a portion of the input records (called the resident partition), while the
 * rest of the records are spilled
 * into the corresponding writers for either disk or network dumping.
 */
public class HybridHashGrouper extends AbstractHistogramPushBasedGrouper {

    protected static final int HT_FRAME_REF_SIZE = 4;
    protected static final int HT_TUPLE_REF_SIZE = 4;
    protected static final int POINTER_INIT_SIZE = 8;
    protected static final int POINTER_LENGTH = 3;

    protected static final int HT_MINI_BLOOM_FILTER_SIZE = 1;
    protected static final int HT_BF_PRIME_FUNC_COUNT = 3;

    protected static final int MAX_RAW_HASHKEY = Integer.MAX_VALUE;

    protected final int tableSize;

    private final IAggregatorDescriptor aggregator, merger;
    private AggregateState aggState;

    private final IBinaryComparator[] comparators;

    private ITuplePartitionComputer tuplePartitionComputer;

    protected final FrameMemManager frameManager;
    protected int[] headers;
    protected int[] hashtablePartitionBuffers;
    protected int[] spilledPartitionBuffers;

    protected RunFileWriter[] spillingPartitionRunWriters;

    private final boolean useMiniBloomFilter;

    private final int spilledPartitions;
    private final int residentPartitions;

    private final long[] recordsInResidentParts, keysInResidentParts;
    private final long[] recordsInSpilledParts;

    private final boolean[] residentPartsSpillFlag;

    private final int outputBuffer;

    private FrameTupleAccessor inputFrameTupleAccessor, groupFrameAccessor;

    private FrameTupleAppender outputAppender;

    private HashTableFrameTupleAppender hashtableFrameTupleAppender;
    private FrameTupleAppender spillFrameTupleAppender;

    private ArrayTupleBuilder groupTupleBuilder, outputTupleBuilder;

    /**
     * For the hash table lookup
     */
    private int lookupFrameIndex, lookupTupleIndex;
    private byte bloomFilterByte;

    private boolean isHashTableFull;

    // counter for the number of tuples that have been processed
    private int processedTuple;

    private List<Long> recordsInRuns;

    private final boolean pinLastPartitionAlways;

    private static final Logger LOGGER = Logger.getLogger(HybridHashGrouper.class.getSimpleName());

    private long debugBloomFilterUpdateCounter = 0, debugBloomFilterLookupCounter = 0, debugOptionalHashHits = 0,
            debugOptionalHashMisses = 0, debugOptionalCPUCompareHit = 0, debugOptionalCPUCompareMiss = 0,
            debugRequiredCPU = 0, debugOptionalSortCPUCompare = 0, debugOptionalSortCPUCopy = 0,
            debugOptionalIOStreamed = 0, debugOptionalIODumped = 0;
    private double debugOptionalMaxHashtableFillRatio = 0;
    private long debugTempCPUCounter = 0, debugTempGroupsInHashtable = 0, debugTempUsedSlots = 0;
    private long profileCPU, profileIOInNetwork, profileIOInDisk, profileIOOutDisk, profileIOOutNetwork,
            profileOutputRecords;
    private long debugBloomFilterSucc = 0, debugBloomFilterFail = 0;

    public HybridHashGrouper(
            IHyracksTaskContext ctx,
            int[] keyFields,
            int[] decorFields,
            int framesLimit,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories,
            IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc,
            boolean enableHistorgram,
            IFrameWriter outputWriter,
            boolean isGenerateRuns,
            int tableSize,
            int spilledPartitions,
            int residentPartitions,
            boolean useBloomFilter,
            boolean pinHighAbsorptionParts,
            boolean alwaysPinLastPart) throws HyracksDataException {
        super(ctx, keyFields, decorFields, framesLimit, aggregatorFactory, mergerFactory, inRecDesc, outRecDesc,
                enableHistorgram, outputWriter, isGenerateRuns);

        this.tableSize = tableSize;

        this.pinLastPartitionAlways = alwaysPinLastPart;

        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < this.comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.tuplePartitionComputer = new FieldHashPartitionComputerFactory(keyFields, hashFunctionFactories)
                .createPartitioner();

        int[] storedKeys = new int[keyFields.length];

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecDesc, outRecDesc, keyFields, storedKeys, null);
        this.aggState = aggregator.createAggregateStates();
        this.merger = mergerFactory.createAggregator(ctx, outRecDesc, outRecDesc, storedKeys, storedKeys, null);

        this.useMiniBloomFilter = useBloomFilter;

        this.spilledPartitions = spilledPartitions;
        this.residentPartitions = residentPartitions;
        this.recordsInResidentParts = new long[this.residentPartitions];
        this.keysInResidentParts = new long[this.residentPartitions];
        this.residentPartsSpillFlag = new boolean[this.residentPartitions];
        // reset all resident parts as in-memory
        for (int i = 0; i < this.residentPartsSpillFlag.length; i++) {
            this.residentPartsSpillFlag[i] = false;
        }

        this.recordsInSpilledParts = new long[this.spilledPartitions];

        this.frameManager = new FrameMemManager(framesLimit, ctx);
        this.outputBuffer = frameManager.allocateFrame();
        if (this.outputBuffer < 0) {
            throw new HyracksDataException("Not enough memory: " + framesLimit);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#
     * init()
     */
    @Override
    public void open() throws HyracksDataException {

        // initialize the hash table
        int headerFramesCount = (int) (Math.ceil((double) tableSize
                * (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0))
                / frameSize));

        if (framesLimit < headerFramesCount + 2) {
            throw new HyracksDataException("Not enough frame (" + framesLimit + ") for a hash table with " + tableSize
                    + " slots.");
        }

        this.headers = new int[headerFramesCount];
        for (int i = 0; i < this.headers.length; i++) {
            this.headers[i] = this.frameManager.allocateFrame();
        }
        resetHeaders();

        if (framesLimit - headers.length - spilledPartitions - 1 <= 0) {
            throw new HyracksDataException("Note enough memory for the hybrid hash algorithm: " + headers.length
                    + " headers and " + spilledPartitions + " partitions.");
        }

        this.hashtablePartitionBuffers = new int[residentPartitions];
        for (int i = 0; i < this.hashtablePartitionBuffers.length; i++) {
            this.hashtablePartitionBuffers[i] = -1;
        }

        // initialize the run file writer array
        this.spillingPartitionRunWriters = new RunFileWriter[this.spilledPartitions];

        // initialize the accessors and appenders
        this.inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inRecDesc);
        this.groupFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        this.groupTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());

        this.hashtableFrameTupleAppender = new HashTableFrameTupleAppender(frameSize, HT_FRAME_REF_SIZE
                + HT_TUPLE_REF_SIZE);

        // reset the lookup reference
        this.lookupFrameIndex = -1;
        this.lookupTupleIndex = -1;

        resetHistogram();

        this.recordsInRuns = new LinkedList<Long>();

        this.spilledPartitionBuffers = new int[this.spilledPartitions];

        this.spillFrameTupleAppender = new FrameTupleAppender(frameSize);

        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());

        this.outputAppender = new FrameTupleAppender(frameSize);
    }

    private void resetHeaders() throws HyracksDataException {
        for (int i = 0; i < this.headers.length; i++) {
            if (this.headers[i] < 0) {
                continue;
            }
            ByteBuffer headerFrame = frameManager.getFrame(this.headers[i]);
            headerFrame.position(0);
            while (headerFrame.position() + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0) + HT_FRAME_REF_SIZE
                    + HT_TUPLE_REF_SIZE < frameSize) {
                if (useMiniBloomFilter) {
                    headerFrame.put((byte) 0);
                }
                headerFrame.putInt(-1);
                headerFrame.putInt(-1);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#
     * nextFrame(java.nio.ByteBuffer, int)
     */
    @Override
    public void nextFrame(
            ByteBuffer buffer) throws HyracksDataException {

        profileIOInNetwork++;

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_INPUT, 1);

        // reset the processed tuple count
        this.processedTuple = 0;

        inputFrameTupleAccessor.reset(buffer);

        int tupleCount = inputFrameTupleAccessor.getTupleCount();

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_INPUT, tupleCount);

        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
            int rawHashValue = tuplePartitionComputer.partition(inputFrameTupleAccessor, tupleIndex, MAX_RAW_HASHKEY);
            int h = rawHashValue % tableSize;
            int residentPartID = h % residentPartitions;

            if (this.residentPartsSpillFlag[residentPartID]) {
                // the resident partition has been spilled, so directly spill the record
                this.groupTupleBuilder.reset();

                for (int i : keyFields) {
                    groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                for (int i : decorFields) {
                    groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                aggregator.init(groupTupleBuilder, inputFrameTupleAccessor, tupleIndex, aggState);

                spillGroup(groupTupleBuilder, h);
            } else {
                if (findMatch(inputFrameTupleAccessor, tupleIndex, rawHashValue, h)) {
                    // match found: do aggregation
                    this.groupFrameAccessor.reset(this.frameManager.getFrame(lookupFrameIndex));
                    int tupleStartOffset = this.groupFrameAccessor.getTupleStartOffset(lookupTupleIndex);
                    int tupleEndOffset = this.groupFrameAccessor.getTupleEndOffset(lookupTupleIndex);
                    this.aggregator.aggregate(inputFrameTupleAccessor, tupleIndex, this.groupFrameAccessor.getBuffer()
                            .array(), tupleStartOffset, tupleEndOffset - tupleStartOffset, aggState);

                    this.recordsInResidentParts[residentPartID]++;
                } else {
                    // not found: if the hash table is not full, insert into the hash table

                    this.groupTupleBuilder.reset();

                    for (int i : keyFields) {
                        groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                    }

                    for (int i : decorFields) {
                        groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                    }

                    aggregator.init(groupTupleBuilder, inputFrameTupleAccessor, tupleIndex, aggState);

                    if (isHashTableFull) {
                        spillGroup(groupTupleBuilder, h);
                        continue;
                    }

                    // insert the new group into the beginning of the slot
                    getSlotPointer(h);

                    if (lookupFrameIndex < 0) {
                        debugTempUsedSlots++;
                    }

                    // check whether the partition to be inserted has space
                    if (this.hashtablePartitionBuffers[residentPartID] < 0) {
                        // try to allocate a new frame
                        this.hashtablePartitionBuffers[residentPartID] = this.frameManager.allocateFrame();
                        while (this.hashtablePartitionBuffers[residentPartID] < 0) {
                            // no more space for this partition; try to find one partition to spill
                            int partIDToSpill = pickPartitionToSpill();
                            if (partIDToSpill >= 0) {
                                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE, partIDToSpill, true);
                            } else {
                                // no more space to recycle
                                break;
                            }
                            this.hashtablePartitionBuffers[residentPartID] = this.frameManager.allocateFrame();
                        }
                        if (this.hashtablePartitionBuffers[residentPartID] < 0) {
                            // failed to find space for this group, so simply spill
                            spillGroup(groupTupleBuilder, h);
                            continue;
                        }
                    }

                    hashtableFrameTupleAppender.reset(
                            this.frameManager.getFrame(this.hashtablePartitionBuffers[residentPartID]), false);
                    if (!hashtableFrameTupleAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                            groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize(), lookupFrameIndex,
                            lookupTupleIndex)) {
                        int newFrameID = this.frameManager.allocateFrame();
                        while (newFrameID < 0) {
                            int partIDToSpill = pickPartitionToSpill();
                            if (partIDToSpill >= 0) {
                                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE, partIDToSpill, true);
                            } else {
                                // no more space to recycle
                                break;
                            }
                            newFrameID = this.frameManager.allocateFrame();
                        }
                        if (newFrameID < 0) {
                            // failed to find space for this group, so simply spill
                            spillGroup(groupTupleBuilder, h);
                            continue;
                        }

                        // Find new frame
                        this.frameManager.setNextFrame(newFrameID, this.hashtablePartitionBuffers[residentPartID]);
                        this.hashtablePartitionBuffers[residentPartID] = newFrameID;
                        hashtableFrameTupleAppender.reset(
                                this.frameManager.getFrame(this.hashtablePartitionBuffers[residentPartID]), true);

                        if (!hashtableFrameTupleAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                                groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize(), lookupFrameIndex,
                                lookupTupleIndex)) {
                            throw new HyracksDataException(
                                    "Failed to insert a group into the hash table: the record is too large.");
                        }
                    }

                    if (useMiniBloomFilter) {
                        bloomFilterByte = insertIntoBloomFilter(rawHashValue, bloomFilterByte, (lookupFrameIndex < 0));
                    }

                    // reset the header reference
                    setSlotPointer(h, bloomFilterByte, this.hashtablePartitionBuffers[residentPartID],
                            hashtableFrameTupleAppender.getTupleCount() - 1);

                    this.recordsInResidentParts[residentPartID]++;
                    this.keysInResidentParts[residentPartID]++;

                    debugTempGroupsInHashtable++;
                }
            }

            insertIntoHistogram(h);
            this.processedTuple++;
        }
    }

    /**
     * Find a resident partition to spill, in order to make extra space.
     * 
     * @return
     */
    private int pickPartitionToSpill() {
        int partIDToSpill = -1;

        int partsInMem = 0;
        double minAbsorptionRatio = Integer.MAX_VALUE;

        for (int i = 0; i < this.residentPartitions; i++) {
            if (this.residentPartsSpillFlag[i]) {
                continue;
            }
            partsInMem++;
            double currentAbsorptionRatio = (this.keysInResidentParts[i] == 0) ? 0 : this.recordsInResidentParts[i]
                    / this.keysInResidentParts[i];
            if (currentAbsorptionRatio < minAbsorptionRatio) {
                minAbsorptionRatio = currentAbsorptionRatio;
                partIDToSpill = i;
            }
        }

        if (partsInMem == 1 && this.pinLastPartitionAlways) {
            // Only one partition is left: pin this partition, and use the hash table output buffer for it
            partIDToSpill = -1;
            // which also means that the hash table is full now
            this.isHashTableFull = true;
        }
        return partIDToSpill;
    }

    private void spillGroup(
            ArrayTupleBuilder tb,
            int hashValue) throws HyracksDataException {
        int partitionToSpill = hashValue % this.spilledPartitions;
        if (this.spilledPartitionBuffers[partitionToSpill] < 0) {
            this.spilledPartitionBuffers[partitionToSpill] = this.frameManager.allocateFrame();
            if (this.spilledPartitionBuffers[partitionToSpill] < 0) {
                throw new HyracksDataException("Error: failed to allocate frame for the spilled partition "
                        + partitionToSpill);
            }
            spillFrameTupleAppender.reset(this.frameManager.getFrame(this.spilledPartitionBuffers[partitionToSpill]),
                    true);
        }
        spillFrameTupleAppender
                .reset(this.frameManager.getFrame(this.spilledPartitionBuffers[partitionToSpill]), false);
        if (!spillFrameTupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            if (isGenerateRuns) {
                // the buffer for this spilled partition is full
                if (spillingPartitionRunWriters[partitionToSpill] == null) {
                    spillingPartitionRunWriters[partitionToSpill] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()), ctx.getIOManager());
                    spillingPartitionRunWriters[partitionToSpill].open();
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                }
                flush(spillingPartitionRunWriters[partitionToSpill], GrouperFlushOption.FLUSH_FOR_GROUP_STATE,
                        partitionToSpill, false);
            } else {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, partitionToSpill, false);
            }
            spillFrameTupleAppender.reset(this.frameManager.getFrame(this.spilledPartitionBuffers[partitionToSpill]),
                    true);
            if (!spillFrameTupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new HyracksDataException("Failed to flush a tuple of a spilling partition");
            }
        }
        this.recordsInSpilledParts[partitionToSpill]++;
    }

    private void setSlotPointer(
            int h,
            byte bfByte,
            int contentFrameIndex,
            int contentTupleIndex) throws HyracksDataException {
        int slotsPerFrame = frameSize
                / (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));
        int slotFrameIndex = h / slotsPerFrame;
        int slotTupleOffset = h % slotsPerFrame
                * (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));

        ByteBuffer headerFrame;

        if (this.headers[slotFrameIndex] < 0) {
            headers[slotFrameIndex] = this.frameManager.allocateFrame();
            if (this.headers[slotFrameIndex] < 0) {
                throw new HyracksDataException("Failed to allocate a frame for header " + slotFrameIndex);
            }
            headerFrame = this.frameManager.getFrame(headers[slotFrameIndex]);
            headerFrame.position(0);
            while (headerFrame.position() + (useMiniBloomFilter ? 9 : 8) < frameSize) {
                if (useMiniBloomFilter) {
                    headerFrame.put((byte) 0);
                }
                headerFrame.putInt(-1);
                headerFrame.putInt(-1);
            }
        }

        headerFrame = this.frameManager.getFrame(headers[slotFrameIndex]);

        if (useMiniBloomFilter) {
            headerFrame.put(slotTupleOffset, bfByte);
        }
        headerFrame.putInt(slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0), contentFrameIndex);
        headerFrame.putInt(slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0) + HT_FRAME_REF_SIZE,
                contentTupleIndex);
    }

    private void getSlotPointer(
            int h) throws HyracksDataException {
        int slotsPerFrame = frameSize
                / (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));
        int slotFrameIndex = h / slotsPerFrame;
        int slotTupleOffset = h % slotsPerFrame
                * (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));

        if (headers[slotFrameIndex] < 0) {
            lookupFrameIndex = -1;
            lookupTupleIndex = -1;
            return;
        }

        ByteBuffer headerFrame = this.frameManager.getFrame(headers[slotFrameIndex]);

        if (useMiniBloomFilter) {
            bloomFilterByte = headerFrame.get(slotTupleOffset);
        }
        lookupFrameIndex = headerFrame.getInt(slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));
        lookupTupleIndex = headerFrame.getInt(slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0)
                + HT_FRAME_REF_SIZE);
    }

    private byte insertIntoBloomFilter(
            int h,
            byte bfByte,
            boolean isInitialize) {
        byte bfByteAfterInsertion = bfByte;
        if (isInitialize) {
            bfByteAfterInsertion = 0;
        }
        for (int i = 0; i < HT_BF_PRIME_FUNC_COUNT; i++) {
            int bitIndex = (int) (h >> (12 * i)) & 0x07;
            bfByteAfterInsertion = (byte) (bfByteAfterInsertion | (1 << bitIndex));
        }
        debugBloomFilterUpdateCounter++;
        return bfByteAfterInsertion;
    }

    private boolean lookupBloomFilter(
            int h,
            byte bfByte) {
        debugBloomFilterLookupCounter++;
        // count each bloom filter as one cpu operation
        debugRequiredCPU++;
        profileCPU++;
        for (int i = 0; i < HT_BF_PRIME_FUNC_COUNT; i++) {
            int bitIndex = (int) (h >> (12 * i)) & 0x07;
            if (!((bfByte & (1L << bitIndex)) != 0)) {
                return false;
            }
        }
        return true;
    }

    private boolean findMatch(
            FrameTupleAccessor accessor,
            int tupleIndex,
            int rawHashValue,
            int tableHashValue) throws HyracksDataException {
        getSlotPointer(tableHashValue);

        if (lookupFrameIndex < 0) {
            return false;
        }

        // do bloom filter lookup, if bloom filter is enabled.
        if (useMiniBloomFilter) {
            if (isHashTableFull) {
                if (!lookupBloomFilter(rawHashValue, bloomFilterByte)) {
                    debugBloomFilterFail++;
                    return false;
                } else {
                    debugBloomFilterSucc++;
                }
            }
        }

        while (lookupFrameIndex >= 0) {
            groupFrameAccessor.reset(this.frameManager.getFrame(lookupFrameIndex));
            if (!sameGroup(accessor, tupleIndex, groupFrameAccessor, lookupTupleIndex)) {
                int tupleEndOffset = groupFrameAccessor.getTupleEndOffset(lookupTupleIndex);
                lookupFrameIndex = groupFrameAccessor.getBuffer().getInt(
                        tupleEndOffset - (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE));
                lookupTupleIndex = groupFrameAccessor.getBuffer().getInt(tupleEndOffset - HT_TUPLE_REF_SIZE);
            } else {
                debugOptionalCPUCompareHit += debugTempCPUCounter;
                debugOptionalHashHits++;
                debugTempCPUCounter = 0;
                return true;
            }
        }
        debugOptionalCPUCompareMiss += debugTempCPUCounter;
        debugOptionalHashMisses++;
        debugTempCPUCounter = 0;

        return false;
    }

    protected boolean sameGroup(
            FrameTupleAccessor a1,
            int t1Idx,
            FrameTupleAccessor a2,
            int t2Idx) {
        debugTempCPUCounter++;
        debugRequiredCPU++;
        profileCPU++;
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = keyFields[i];
            int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength() + a1.getFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength() + a2.getFieldStartOffset(t2Idx, i);
            int l2 = a2.getFieldLength(t2Idx, i);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    private void flush(
            IFrameWriter writer,
            IGrouperFlushOption flushOption,
            int partitionIndex,
            boolean isResidentPart) throws HyracksDataException {

        outputAppender.reset(this.frameManager.getFrame(outputBuffer), true);

        int bufToFlush, prevFrameID;

        IAggregatorDescriptor aggDesc;

        if (flushOption.getOutputState() == GroupOutputState.GROUP_STATE) {
            aggDesc = aggregator;
        } else if (flushOption.getOutputState() == GroupOutputState.RESULT_STATE) {
            aggDesc = merger;
        } else {
            throw new HyracksDataException("Cannot output " + GroupOutputState.RAW_STATE.name()
                    + " for flushing hybrid hash grouper");
        }

        if (isResidentPart) {
            bufToFlush = this.hashtablePartitionBuffers[partitionIndex];
        } else {
            bufToFlush = this.spilledPartitionBuffers[partitionIndex];
        }

        while (bufToFlush >= 0) {
            groupFrameAccessor.reset(this.frameManager.getFrame(bufToFlush));
            int tupleCount = groupFrameAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                outputTupleBuilder.reset();
                for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                    outputTupleBuilder.addField(groupFrameAccessor, i, k);
                }
                aggDesc.outputFinalResult(outputTupleBuilder, groupFrameAccessor, i, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
                    FrameUtils.flushFrame(this.frameManager.getFrame(outputBuffer), writer);

                    if (isGenerateRuns) {
                        profileIOOutDisk++;
                    } else {
                        profileIOOutNetwork++;
                    }

                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                            outputAppender.getTupleCount());
                    if (flushOption == GrouperFlushOption.FLUSH_FOR_GROUP_STATE) {
                        debugOptionalIODumped++;
                    } else {
                        debugOptionalIOStreamed++;
                    }
                    outputAppender.reset(this.frameManager.getFrame(outputBuffer), true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException(
                                "Failed to dump a group from the hash table to a frame: possibly the size of the tuple is too large.");
                    }
                }
            }
            prevFrameID = bufToFlush;
            bufToFlush = this.frameManager.getNextFrame(bufToFlush);
            if (isResidentPart) {
                // recycle the frame for the resident partition
                this.frameManager.recycleFrame(prevFrameID);
            }
        }

        if (outputAppender.getTupleCount() > 0) {
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
            FrameUtils.flushFrame(this.frameManager.getFrame(outputBuffer), writer);
            if (isGenerateRuns && partitionIndex >= 0) {
                profileIOOutDisk++;
            } else {
                profileIOOutNetwork++;
            }
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                    outputAppender.getTupleCount());
            if (flushOption == GrouperFlushOption.FLUSH_FOR_GROUP_STATE) {
                debugOptionalIODumped++;
            } else {
                debugOptionalIOStreamed++;
            }
            outputAppender.reset(this.frameManager.getFrame(outputBuffer), true);
        }

        if (isResidentPart) {
            this.residentPartsSpillFlag[partitionIndex] = true;
        }
    }

    @Override
    public void wrapup() throws HyracksDataException {

        if (outputAppender == null) {
            outputAppender = new FrameTupleAppender(frameSize);
        }

        for (int i = 0; i < this.spilledPartitions; i++) {
            if (spilledPartitionBuffers[i] < 0) {
                continue;
            }
            groupFrameAccessor.reset(this.frameManager.getFrame(spilledPartitionBuffers[i]));
            if (groupFrameAccessor.getTupleCount() == 0) {
                continue;
            }
            if (isGenerateRuns) {
                if (spillingPartitionRunWriters[i] == null) {
                    spillingPartitionRunWriters[i] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()), ctx.getIOManager());
                    spillingPartitionRunWriters[i].open();
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                }
                flush(spillingPartitionRunWriters[i], GrouperFlushOption.FLUSH_FOR_GROUP_STATE, i, false);
                runReaders.add(spillingPartitionRunWriters[i].createReader());
                recordsInRuns.add(this.recordsInSpilledParts[i]);
                spillingPartitionRunWriters[i].close();
            } else {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, i, false);
            }
        }

        // flush the resident partition
        // FIXME if the result can be detected as the final result, they may not need to be sent
        // through network again
        for (int i = 0; i < this.residentPartitions; i++) {
            if (!this.residentPartsSpillFlag[i]) {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, i, true);
            }
        }
    }

    @Override
    protected void flush(
            IFrameWriter writer,
            GrouperFlushOption flushOption) throws HyracksDataException {
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#
     * reset()
     */
    @Override
    public void reset() throws HyracksDataException {

        super.reset();

        for (int i = 0; i < this.residentPartitions; i++) {
            this.recordsInResidentParts[i] = 0;
            this.keysInResidentParts[i] = 0;
        }

        for (int i = 0; i < this.spilledPartitions; i++) {
            this.recordsInSpilledParts[i] = 0;
        }

        this.isHashTableFull = false;

        // reset the lookup reference
        this.lookupFrameIndex = -1;
        this.lookupTupleIndex = -1;

        // reset header pages
        resetHeaders();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#
     * close()
     */
    @Override
    public void close() throws HyracksDataException {

        dumpAndCleanDebugCounters();

        // flush the resident partition (all contents in the hash table)

        this.headers = null;
        this.spilledPartitionBuffers = null;
    }

    public int getProcessedTupleCount() {
        return this.processedTuple;
    }

    protected enum HybridHashFlushOption implements IGrouperFlushOption {

        FLUSH_SPILLED_PARTITION_FOR_GROUP_STATE(GroupOutputState.GROUP_STATE),
        FLUSH_SPILLED_PARTITION_FOR_RESULT_STATE(GroupOutputState.RESULT_STATE),
        FLUSH_HASHTABLE_FOR_GROUP_STATE(GroupOutputState.GROUP_STATE),
        FLUSH_HASHTABLE_FOR_RESULT_STATE(GroupOutputState.RESULT_STATE);

        private final GroupOutputState outputState;

        private HybridHashFlushOption(
                GroupOutputState outputState) {
            this.outputState = outputState;
        }

        @Override
        public GroupOutputState getOutputState() {
            return this.outputState;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    public long getRawRecordsInResidentPartition() {
        long rawRecordsInResidentPartition = 0;
        for (int i = 0; i < this.residentPartitions; i++) {
            if (!this.residentPartsSpillFlag[i]) {
                rawRecordsInResidentPartition += this.recordsInResidentParts[i];
            }
        }
        return rawRecordsInResidentPartition;
    }

    public long getGroupsInResidentPartition() {
        long groupsInResidentPartition = 0;
        for (int i = 0; i < this.residentPartitions; i++) {
            if (!this.residentPartsSpillFlag[i]) {
                groupsInResidentPartition += this.keysInResidentParts[i];
            }
        }
        return groupsInResidentPartition;
    }

    public List<Long> getRawRecordsInSpillingPartitions() {
        return recordsInRuns;
    }

    protected void dumpAndCleanDebugCounters() {

        this.debugCounters.updateRequiredCounter(RequiredCounters.CPU, debugRequiredCPU);
        this.debugCounters.updateRequiredCounter(RequiredCounters.IO_OUT_DISK, debugOptionalIODumped);

        this.debugCounters.updateOptionalCustomizedCounter(".io.streamed", debugOptionalIOStreamed);
        this.debugCounters.updateOptionalCustomizedCounter(".io.dumped", debugOptionalIODumped);

        this.debugCounters.updateOptionalCustomizedCounter(".hash.bloomfilter.update", debugBloomFilterUpdateCounter);
        this.debugCounters.updateOptionalCustomizedCounter(".hash.bloomfilter.lookup", debugBloomFilterLookupCounter);

        this.debugCounters.updateOptionalCustomizedCounter(".hash.groupsInTable", debugTempGroupsInHashtable);
        this.debugCounters.updateOptionalCustomizedCounter(".hash.slotsUsed", debugTempUsedSlots);

        this.debugCounters.updateOptionalCustomizedCounter(".bloomfilter.succ", debugBloomFilterSucc);
        this.debugCounters.updateOptionalCustomizedCounter(".bloomfilter.fail", debugBloomFilterFail);

        debugOptionalMaxHashtableFillRatio = Math.max(debugOptionalMaxHashtableFillRatio,
                (double) debugTempGroupsInHashtable / (double) debugTempUsedSlots);

        if (debugOptionalMaxHashtableFillRatio > 2) {
            System.out.println("Bad Hash Table!");
        }

        this.debugCounters.updateOptionalCustomizedCounter(".hash.maxFillRatio",
                ((long) debugOptionalMaxHashtableFillRatio * 100));

        this.debugCounters.updateOptionalSortCounter(OptionalSortCounters.CPU_COMPARE, debugOptionalSortCPUCompare);
        this.debugCounters.updateOptionalSortCounter(OptionalSortCounters.CPU_COPY, debugOptionalSortCPUCopy);

        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.CPU_COMPARE_HIT, debugOptionalCPUCompareHit);
        this.debugCounters
                .updateOptionalHashCounter(OptionalHashCounters.CPU_COMPARE_MISS, debugOptionalCPUCompareMiss);
        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.CPU_COMPARE, debugOptionalCPUCompareHit
                + debugOptionalCPUCompareMiss);
        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.HITS, debugOptionalHashHits);
        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.MISSES, debugOptionalHashMisses);

        this.debugCounters.dumpCounters(ctx.getCounterContext());

        this.debugRequiredCPU = 0;
        this.debugOptionalIODumped = 0;
        this.debugOptionalIOStreamed = 0;
        this.debugOptionalSortCPUCompare = 0;
        this.debugOptionalSortCPUCopy = 0;
        this.debugOptionalHashHits = 0;
        this.debugOptionalHashMisses = 0;
        this.debugOptionalCPUCompareHit = 0;
        this.debugOptionalCPUCompareMiss = 0;
        this.debugOptionalMaxHashtableFillRatio = 0;
        this.debugBloomFilterLookupCounter = 0;
        this.debugBloomFilterUpdateCounter = 0;
        this.debugTempCPUCounter = 0;
        this.debugTempGroupsInHashtable = 0;
        this.debugTempUsedSlots = 0;
        this.debugBloomFilterSucc = 0;
        this.debugBloomFilterFail = 0;
        this.debugCounters.reset();

        ctx.getCounterContext().getCounter("profile.cpu." + this.debugCounters.getDebugID(), true).update(profileCPU);
        ctx.getCounterContext().getCounter("profile.io.in.disk." + this.debugCounters.getDebugID(), true)
                .update(profileIOInDisk);
        ctx.getCounterContext().getCounter("profile.io.in.network." + this.debugCounters.getDebugID(), true)
                .update(profileIOInNetwork);
        ctx.getCounterContext().getCounter("profile.io.out.disk." + this.debugCounters.getDebugID(), true)
                .update(profileIOOutDisk);
        ctx.getCounterContext().getCounter("profile.io.out.network." + this.debugCounters.getDebugID(), true)
                .update(profileIOOutNetwork);
        ctx.getCounterContext().getCounter("profile.output." + this.debugCounters.getDebugID(), true)
                .update(profileOutputRecords);

        profileCPU = 0;
        profileIOInDisk = 0;
        profileIOInNetwork = 0;
        profileIOOutDisk = 0;
        profileIOOutNetwork = 0;
        profileOutputRecords = 0;

    }
}
