/*
 * Copyright 2009-2014 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class DynamicHybridHashGrouper extends AbstractHistogramPushBasedGrouper {

    /**
     * The length of hash table frame reference
     */
    protected static final int HT_FRAME_REF_SIZE = 4;

    /**
     * The length of hash table tuple reference
     */
    protected static final int HT_TUPLE_REF_SIZE = 4;

    /**
     * The byte for mini bloom-filter
     */
    protected static final int HT_MINI_BLOOM_FILTER_SIZE = 1;

    /**
     * The number of hashes for the prime function
     */
    protected static final int HT_BF_PRIME_FUNC_COUNT = 3;

    protected static final int MIN_FRAMES_PER_RESIDENT_PART = 3;

    protected static final int MAX_RAW_HASHKEY = Integer.MAX_VALUE;

    /**
     * Hyracks task context.
     */
    protected final IHyracksTaskContext ctx;

    /**
     * The size of each frame.
     */
    private final int frameSize;

    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IAggregatorDescriptorFactory aggregatorFactory, mergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;

    /**
     * The output writer
     */
    private final IFrameWriter outputWriter;

    /**
     * Group-by key columns.
     */
    protected final int[] keyFields;

    /**
     * Group-by decor columns.
     */
    protected final int[] decorFields;

    /**
     * The number of hash table slots.
     */
    private final int tableSize;

    /**
     * The number of partitions.
     */
    private final int numParts;

    /**
     * The list of output buffers for the spilled partitions
     */
    private final int[] partitionBuffers;

    private final int[] partitionBufferCounters;

    private final int[] partitionOutputBuffers;

    private final boolean[] partitionSpillingFlags;

    /**
     * The flag on whether the last in-memory partition should be pinned, even
     * its absorption has fallen below the average absorption ratio.
     */
    private final boolean pinLastPartitionAlways;

    private final boolean[] partitionPinFlags;

    /**
     * The frame manager to manage the buffers.
     */
    private final FrameMemManager frameManager;

    private final int hashtableOutputBuffer;

    /**
     * Buffer IDs for the hash table headers. Each element in this array is a
     * frame ID from the frameManager.
     */
    private int[] htHeaderFrames;

    /**
     * The number of raw records and keys in each resident partition. These two
     * arrays are used to pick the resident partition with lowest absorption
     * ratio to spill.
     */
    private long[] recordsInParts, groupsInParts;

    /**
     * The total number of records and groups in memory and inserted. The counters in memory
     * could provide an upper bound of the absorption ratio, while the counters in total is
     * a lower bound of the absorption ratio. In details,
     * - thresholdTotalRecordsInMem
     */
    private long thresholdTotalRecordsInMem, thresholdTotalGroupsInMem;
    private long thresholdTotalRecordsInserted, thresholdTotalGroupsInserted;

    /**
     * Whether to use mini-bloomfilter to enhance the hashtable lookup
     */
    private final boolean useMiniBloomFilter;

    private int[] keysInPartialResult;

    private IBinaryComparator[] comparators;
    private ITuplePartitionComputer rawTuplePartitionComputer, partialTuplePartitionComputer;
    private IAggregatorDescriptor aggregator, merger;
    private IBinaryHashFunctionFactory[] hashFunctionFactories;
    private AggregateState aggState;

    /**
     * Frame accessors: inputFrameTupleAccessor is for the input frame, and
     * groupFrameTupleAccessor is for the group state.
     */
    private FrameTupleAccessor inputFrameTupleAccessor, groupFrameTupleAccessor;

    /**
     * Tuple builders: groupTupleBuild is for the group state in memory, and
     * outputTupleBuilder is for the flushing state out of the memory.
     */
    private ArrayTupleBuilder groupTupleBuilder, outputTupleBuilder;

    private HashTableFrameTupleAppender hashtableFrameTupleAppender;

    private FrameTupleAppender spillFrameTupleAppender, outputFrameTupleAppender;

    /**
     * The hashtable lookup index for frame and tuple
     */
    private int htLookupFrameIndex = -1, htLookupTupleIndex = -1;

    /**
     * The bloomfilter lookup cache to store the bloomfilter byte found.
     */
    private byte bloomFilterByte = (byte) -1;

    /**
     * the list of unpinned partition ids.
     */
    private List<Integer> unpinnedSpilledParts;

    /**
     * Flags to generate run files for spilled partitions
     */
    private final boolean isGenerateRuns;

    /**
     * The flag for whether the hash table is full (only one resident partition is pinned)
     */
    private boolean isHashtableFull = false;

    private final RunFileWriter[] partSpillRunWriters;

    private List<Long> recordsInsertedForParts, keysInsertedForParts;
    private long recordsInResidentParts, groupsInResidentParts;

    private static final Logger LOGGER = Logger.getLogger(DynamicHybridHashGrouper.class.getSimpleName());

    private long debugBloomFilterUpdateCounter = 0, debugBloomFilterLookupCounter = 0, debugOptionalHashHits = 0,
            debugOptionalHashMisses = 0, debugOptionalCPUCompareHit = 0, debugOptionalCPUCompareMiss = 0,
            debugRequiredCPU = 0, debugOptionalSortCPUCompare = 0, debugOptionalSortCPUCopy = 0,
            debugOptionalIOStreamed = 0, debugOptionalIODumped = 0;
    private double debugOptionalMaxHashtableFillRatio = 0;
    private long debugTempCPUCounter = 0, debugTempGroupsInHashtable = 0, debugTempUsedSlots = 0;
    private long profileCPU, profileIOInNetwork, profileIOInDisk, profileIOOutDisk, profileIOOutNetwork,
            profileOutputRecords;

    private long profileInRecords, profileInFrames, profileOutRecords, profileOutFrames;

    private long debugBloomFilterSucc = 0, debugBloomFilterFail = 0;

    public enum PartSpillStrategy {
        MIN_FIRST,
        MAX_FIRST,
        MIN_ABSORB_FIRST,
        LOWER_ABSORB_THAN_AVG
    }

    private final PartSpillStrategy spillStrategy;

    public DynamicHybridHashGrouper(
            IHyracksTaskContext ctx,
            int[] keyFields,
            int[] decorFields,
            int framesLimit,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories,
            IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor,
            boolean enableHistorgram,
            IFrameWriter outputWriter,
            boolean isGenerateRuns,
            int tableSize,
            int numParts,
            boolean useMiniBloomFilter,
            boolean alwaysPinLastPart,
            PartSpillStrategy partSpillStrategy) throws HyracksDataException {

        super(ctx, keyFields, decorFields, framesLimit, aggregatorFactory, mergerFactory, inRecordDescriptor,
                outRecordDescriptor, enableHistorgram, outputWriter, isGenerateRuns);

        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.frameSize = ctx.getFrameSize();
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;
        this.outputWriter = outputWriter;
        this.hashFunctionFactories = hashFunctionFactories;

        this.tableSize = tableSize;

        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;
        this.pinLastPartitionAlways = alwaysPinLastPart;

        this.useMiniBloomFilter = useMiniBloomFilter;

        this.spillStrategy = partSpillStrategy;

        this.numParts = numParts;
        this.partitionBuffers = new int[this.numParts];
        this.partitionSpillingFlags = new boolean[this.numParts];
        this.partitionPinFlags = new boolean[this.numParts];
        this.partitionOutputBuffers = new int[this.numParts];
        this.recordsInParts = new long[this.numParts];
        this.groupsInParts = new long[this.numParts];
        this.partitionBufferCounters = new int[this.numParts];
        for (int i = 0; i < this.numParts; i++) {
            this.partitionSpillingFlags[i] = false;
            this.partitionPinFlags[i] = false;
            this.partitionBuffers[i] = -1;
            this.partitionOutputBuffers[i] = -1;
            this.partitionBufferCounters[i] = 0;
        }
        this.recordsInsertedForParts = new LinkedList<Long>();
        this.keysInsertedForParts = new LinkedList<Long>();

        this.isGenerateRuns = isGenerateRuns;
        this.partSpillRunWriters = new RunFileWriter[this.numParts];

        this.frameManager = new FrameMemManager(framesLimit, ctx);
        this.hashtableOutputBuffer = frameManager.allocateFrame();

        // Reserve one frame for each partition
        for (int i = 0; i < this.numParts; i++) {
            this.partitionBuffers[i] = frameManager.allocateFrame();
            this.partitionBufferCounters[i]++;
            if (this.partitionBuffers[i] < 0) {
                throw new HyracksDataException("Not enough memory for " + this.numParts + " partitions.");
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {

        this.keysInPartialResult = new int[this.keyFields.length];
        for (int i = 0; i < this.keysInPartialResult.length; i++) {
            this.keysInPartialResult[i] = i;
        }

        this.comparators = new IBinaryComparator[this.comparatorFactories.length];
        for (int i = 0; i < this.comparators.length; i++) {
            this.comparators[i] = this.comparatorFactories[i].createBinaryComparator();
        }

        this.rawTuplePartitionComputer = new FieldHashPartitionComputerFactory(keyFields, hashFunctionFactories)
                .createPartitioner();
        this.partialTuplePartitionComputer = new FieldHashPartitionComputerFactory(keysInPartialResult,
                hashFunctionFactories).createPartitioner();

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDesc, outRecordDesc, keyFields,
                keysInPartialResult, null);
        this.aggState = aggregator.createAggregateStates();

        this.merger = mergerFactory.createAggregator(ctx, outRecordDesc, outRecordDesc, keysInPartialResult,
                keysInPartialResult, null);

        this.inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inRecordDesc);
        this.groupFrameTupleAccessor = new FrameTupleAccessor(frameSize, outRecordDesc);

        this.groupTupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFieldCount());

        this.hashtableFrameTupleAppender = new HashTableFrameTupleAppender(frameSize, HT_FRAME_REF_SIZE
                + HT_TUPLE_REF_SIZE);

        this.spillFrameTupleAppender = new FrameTupleAppender(frameSize);
        this.outputFrameTupleAppender = new FrameTupleAppender(frameSize);

        int slotsPerFrame = frameSize
                / ((useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0) + HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE);
        int htHeadersCount = (int) Math.ceil((double) this.tableSize / slotsPerFrame);
        this.htHeaderFrames = new int[htHeadersCount];
        for (int i = 0; i < this.htHeaderFrames.length; i++) {
            this.htHeaderFrames[i] = frameManager.allocateFrame();
            if (this.htHeaderFrames[i] < 0) {
                throw new HyracksDataException("Not enough memory for in-mem hash table headers.");
            }
        }
        resetHeaders();

        this.unpinnedSpilledParts = new LinkedList<Integer>();

    }

    private void resetHeaders() throws HyracksDataException {
        for (int i = 0; i < this.htHeaderFrames.length; i++) {
            if (this.htHeaderFrames[i] < 0) {
                continue;
            }
            ByteBuffer headerFrame = frameManager.getFrame(this.htHeaderFrames[i]);
            headerFrame.position(0);
            while (headerFrame.position() + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0) + HT_FRAME_REF_SIZE
                    + HT_TUPLE_REF_SIZE <= frameSize) {
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
     * edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(
            ByteBuffer buffer) throws HyracksDataException {

        profileIOInNetwork++;

        profileInFrames++;
        profileInRecords += buffer.getInt(buffer.capacity() - INT_SIZE);

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_INPUT, 1);

        this.inputFrameTupleAccessor.reset(buffer);
        int tupleCount = this.inputFrameTupleAccessor.getTupleCount();

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_INPUT, tupleCount);

        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
            int rawHashValue = rawTuplePartitionComputer
                    .partition(inputFrameTupleAccessor, tupleIndex, MAX_RAW_HASHKEY);
            int htSlotID = rawHashValue % this.tableSize;

            this.thresholdTotalRecordsInserted++;

            /**
             * Need to partition data based on their hash table slot id, so that different groups having
             * the same hash table slot id will in the same partition.
             */
            int partID = htSlotID % this.numParts;
            if (this.partitionSpillingFlags[partID]) {
                // partition is spilled
                int spillPartID = partID;
                if (this.partitionPinFlags[partID]) {
                    // partition has been pinned, then it will be partitioned
                    // into other non-pinned spilled partitions
                    throw new HyracksDataException("Should not reach this right now!");
                    // spillPartID = getUnpinnedSpilledPartIDForSpilling(htSlotID);
                }

                this.groupTupleBuilder.reset();

                for (int i : keyFields) {
                    groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                for (int i : decorFields) {
                    groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                aggregator.init(groupTupleBuilder, inputFrameTupleAccessor, tupleIndex, aggState);

                spillGroup(groupTupleBuilder, spillPartID);
                this.recordsInParts[spillPartID]++;
                this.groupsInParts[spillPartID]++;
                this.thresholdTotalGroupsInserted++;

            } else {
                // The partition is not spilled: try to find a match first
                if (findMatch(inputFrameTupleAccessor, tupleIndex, rawHashValue, htSlotID)) {
                    // found a match: update the group-by state
                    this.groupFrameTupleAccessor.reset(frameManager.getFrame(htLookupFrameIndex));
                    int tupleStartOffset = this.groupFrameTupleAccessor.getTupleStartOffset(htLookupTupleIndex);
                    int tupleEndOffset = this.groupFrameTupleAccessor.getTupleEndOffset(htLookupTupleIndex);
                    this.aggregator.aggregate(inputFrameTupleAccessor, tupleIndex,
                            frameManager.getFrame(htLookupFrameIndex).array(), tupleStartOffset, tupleEndOffset
                                    - tupleStartOffset, aggState);
                    this.recordsInParts[partID]++;
                    this.thresholdTotalRecordsInMem++;
                } else {
                    // haven't found a match: either spill (for pinned part) or
                    // add a new entry (for unpinned part)

                    // Always inserted into a new group
                    this.thresholdTotalGroupsInserted++;

                    this.groupTupleBuilder.reset();

                    for (int i : keyFields) {
                        groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                    }

                    for (int i : decorFields) {
                        groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                    }

                    aggregator.init(groupTupleBuilder, inputFrameTupleAccessor, tupleIndex, aggState);

                    if (this.isHashtableFull) {
                        int unpinnedPartPicked = getUnpinnedSpilledPartIDForSpilling(htSlotID);
                        spillGroup(groupTupleBuilder, unpinnedPartPicked);
                        this.recordsInParts[unpinnedPartPicked]++;
                        this.groupsInParts[unpinnedPartPicked]++;
                    } else {

                        boolean isInsertedResidentPart = true;
                        // try to insert a new entry
                        getHTSlotPointer(htSlotID);
                        if (this.partitionBuffers[partID] >= 0) {
                            hashtableFrameTupleAppender.reset(frameManager.getFrame(this.partitionBuffers[partID]),
                                    false);
                        }
                        while (this.partitionBuffers[partID] < 0
                                || !hashtableFrameTupleAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                                        groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize(),
                                        htLookupFrameIndex, htLookupTupleIndex)) {

                            boolean isAllocated = allocateBufferForPart(partID);
                            if (this.partitionSpillingFlags[partID]) {
                                spillGroup(groupTupleBuilder, partID);
                                this.recordsInParts[partID]++;
                                this.groupsInParts[partID]++;
                                isInsertedResidentPart = false;
                                break;
                            } else {
                                if (!isAllocated) {
                                    // must be the last partition pinned
                                    if (this.isHashtableFull) {
                                        int unpinnedPartPicked = getUnpinnedSpilledPartIDForSpilling(htSlotID);
                                        spillGroup(groupTupleBuilder, unpinnedPartPicked);
                                        this.recordsInParts[unpinnedPartPicked]++;
                                        this.groupsInParts[unpinnedPartPicked]++;
                                        isInsertedResidentPart = false;
                                        break;
                                    }
                                } else {
                                    hashtableFrameTupleAppender.reset(
                                            frameManager.getFrame(this.partitionBuffers[partID]), true);
                                }
                            }
                        }
                        if (isInsertedResidentPart) {
                            // successfully inserted
                            if (useMiniBloomFilter) {
                                bloomFilterByte = insertIntoBloomFilter(rawHashValue, bloomFilterByte,
                                        (htLookupFrameIndex < 0));
                            }

                            // reset the header reference
                            setHTSlotPointer(htSlotID, bloomFilterByte, this.partitionBuffers[partID],
                                    hashtableFrameTupleAppender.getTupleCount() - 1);
                            debugTempGroupsInHashtable++;

                            this.recordsInParts[partID]++;
                            this.groupsInParts[partID]++;

                            this.thresholdTotalGroupsInMem++;
                            this.thresholdTotalRecordsInMem++;

                        }

                    }
                }
            }
        }
    }

    private void getHTSlotPointer(
            int htSlotID) throws HyracksDataException {
        int slotsPerFrame = frameSize
                / (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));
        int slotFrameIndex = htSlotID / slotsPerFrame;
        int slotTupleOffset = htSlotID % slotsPerFrame
                * (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));

        if (this.htHeaderFrames[slotFrameIndex] < 0) {
            htLookupFrameIndex = -1;
            htLookupTupleIndex = -1;
            return;
        }

        if (useMiniBloomFilter) {
            bloomFilterByte = frameManager.getFrame(this.htHeaderFrames[slotFrameIndex]).get(slotTupleOffset);
        }
        htLookupFrameIndex = frameManager.getFrame(this.htHeaderFrames[slotFrameIndex]).getInt(
                slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));
        htLookupTupleIndex = frameManager.getFrame(this.htHeaderFrames[slotFrameIndex]).getInt(
                slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0) + HT_FRAME_REF_SIZE);
    }

    /**
     * Update the hash table slot
     * 
     * @param htSlotID
     * @param bloomFilterByte
     * @param frameIndex
     * @param tupleIndex
     * @throws HyracksDataException
     */
    private void setHTSlotPointer(
            int htSlotID,
            byte bloomFilterByte,
            int frameIndex,
            int tupleIndex) throws HyracksDataException {
        int slotsPerFrame = frameSize
                / (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));
        int slotFrameIndex = htSlotID / slotsPerFrame;
        int slotTupleOffset = htSlotID % slotsPerFrame
                * (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));

        if (this.htHeaderFrames[slotFrameIndex] < 0) {
            this.htHeaderFrames[slotFrameIndex] = frameManager.allocateFrame();
            if (this.htHeaderFrames[slotFrameIndex] < 0) {
                throw new HyracksDataException("Failed to allocate frame for hash table headers");
            }
            frameManager.resetFrame(this.htHeaderFrames[slotFrameIndex]);
        }

        if (useMiniBloomFilter) {
            frameManager.getFrame(this.htHeaderFrames[slotFrameIndex]).put(slotTupleOffset, bloomFilterByte);
        }
        frameManager.getFrame(this.htHeaderFrames[slotFrameIndex]).putInt(
                slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0), frameIndex);
        frameManager.getFrame(this.htHeaderFrames[slotFrameIndex]).putInt(
                slotTupleOffset + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0) + HT_FRAME_REF_SIZE, tupleIndex);
    }

    /**
     * Try to allocate a frame to the given partition. Return true if a new
     * frame is allocated successfully, otherwise return false.
     * 
     * @param partID
     * @return
     * @throws HyracksDataException
     */
    private boolean allocateBufferForPart(
            int partID) throws HyracksDataException {
        int newFrameID = frameManager.allocateFrame();
        while (newFrameID < 0) {
            // no more free frames from the pool of the frame manager
            int partIDToSpill = pickPartitionToSpill();
            if (partIDToSpill >= 0) {
                // Spill one partition
                if (isGenerateRuns) {
                    if (this.partSpillRunWriters[partIDToSpill] == null) {
                        this.partSpillRunWriters[partIDToSpill] = new RunFileWriter(
                                ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()),
                                ctx.getIOManager());
                        this.partSpillRunWriters[partIDToSpill].open();
                        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                    }
                    flush(this.partSpillRunWriters[partIDToSpill], GrouperFlushOption.FLUSH_FOR_GROUP_STATE,
                            partIDToSpill);
                } else {
                    flush(outputWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE, partIDToSpill);
                }
                if (partIDToSpill != partID) {
                    newFrameID = frameManager.allocateFrame();
                } else {
                    return false;
                }
            } else {
                break;
            }
        }

        if (!this.partitionSpillingFlags[partID] && newFrameID >= 0) {
            frameManager.setNextFrame(newFrameID, this.partitionBuffers[partID]);
            this.partitionBuffers[partID] = newFrameID;
            this.partitionBufferCounters[partID]++;
            return true;
        } else {
            if (this.unpinnedSpilledParts.size() == this.numParts - 1) {
                this.isHashtableFull = true;
            }
            return false;
        }
    }

    /**
     * Compute the estimated absorption ratio, as the average between the upper bound (groups/records in memory) and
     * the lower bound (groups/records totally).
     * 
     * @return
     */
    private double getEstimatedAbsorptionRatio() {
        assert (thresholdTotalGroupsInMem <= thresholdTotalRecordsInMem);
        assert (thresholdTotalGroupsInserted <= thresholdTotalRecordsInserted);
        return 1 - (((double) thresholdTotalGroupsInMem) / ((double) thresholdTotalRecordsInMem) + ((double) thresholdTotalGroupsInserted)
                / ((double) thresholdTotalRecordsInserted)) / 2;
    }

    private int pickPartitionToSpill() {
        int partIDToSpill = -1;
        boolean pickMin = true;
        int partsInMem = 0;

        double estimatedAbsorptionRatio = getEstimatedAbsorptionRatio();

        switch (this.spillStrategy) {
            case MAX_FIRST:
                pickMin = false;
            case MIN_FIRST:
                pickMin = true;
                int partBufCount = pickMin ? Integer.MAX_VALUE : Integer.MIN_VALUE;
                for (int i = 0; i < this.numParts; i++) {
                    if (this.partitionSpillingFlags[i]) {
                        continue;
                    }
                    partsInMem++;
                    if (pickMin ? (this.partitionBufferCounters[i] <= partBufCount)
                            : (this.partitionBufferCounters[i] >= partBufCount)) {
                        partIDToSpill = i;
                        partBufCount = this.partitionBufferCounters[i];
                    }
                }
                break;
            case LOWER_ABSORB_THAN_AVG:
                for (int i = 0; i < this.numParts; i++) {
                    if (this.partitionSpillingFlags[i]) {
                        continue;
                    }
                    partsInMem++;
                    double currentAbsorptionRatio = 1 - ((this.groupsInParts[i] == 0) ? 1 : this.recordsInParts[i]
                            / this.groupsInParts[i]);
                    if (currentAbsorptionRatio > 0 && currentAbsorptionRatio < estimatedAbsorptionRatio) {
                        partIDToSpill = i;
                        break;
                    }
                }
                break;
            case MIN_ABSORB_FIRST:
            default:
                double minAbsorptionRatio = Integer.MAX_VALUE;

                for (int i = 0; i < this.numParts; i++) {
                    if (this.partitionSpillingFlags[i]) {
                        continue;
                    }
                    partsInMem++;
                    double currentAbsorptionRatio = (this.groupsInParts[i] == 0) ? 0 : this.recordsInParts[i]
                            / this.groupsInParts[i];
                    if (currentAbsorptionRatio < minAbsorptionRatio) {
                        minAbsorptionRatio = currentAbsorptionRatio;
                        partIDToSpill = i;
                    }
                }
        }
        // Only one partition is left: pin this partition if required, and use the hash table output buffer for it
        if (partIDToSpill == -1 || (partIDToSpill >= 0 && partsInMem == 1 && this.pinLastPartitionAlways)) {
            if (partIDToSpill >= 0) {
                this.partitionPinFlags[partIDToSpill] = true;
            }
            this.isHashtableFull = true;
            partIDToSpill = -1;
        }

        return partIDToSpill;
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

    /**
     * Spill a record into the given spilled partition.
     * 
     * @param partID
     * @return
     * @throws HyracksDataException
     */
    private int getUnpinnedSpilledPartIDForSpilling(
            int partID) throws HyracksDataException {
        if (this.unpinnedSpilledParts.size() == 0) {
            throw new HyracksDataException("Should not reach this!");
        }
        int unpinnedPartIDInList = partID % this.unpinnedSpilledParts.size();
        return this.unpinnedSpilledParts.get(unpinnedPartIDInList);
    }

    /**
     * Spill a group state into the given partition.
     * 
     * @param tb
     * @param spillPartID
     * @throws HyracksDataException
     */
    private void spillGroup(
            ArrayTupleBuilder tb,
            int spillPartID) throws HyracksDataException {
        if (this.partitionOutputBuffers[spillPartID] < 0) {
            this.partitionOutputBuffers[spillPartID] = frameManager.allocateFrame();
            if (this.partitionOutputBuffers[spillPartID] < 0) {
                throw new HyracksDataException("Not enough frame for spilling partition " + spillPartID);
            }
            spillFrameTupleAppender.reset(frameManager.getFrame(this.partitionOutputBuffers[spillPartID]), true);
        } else {
            spillFrameTupleAppender.reset(frameManager.getFrame(this.partitionOutputBuffers[spillPartID]), false);
        }
        if (!spillFrameTupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            if (isGenerateRuns) {
                // the buffer for this spilled partition is full
                if (this.partSpillRunWriters[spillPartID] == null) {
                    this.partSpillRunWriters[spillPartID] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()), ctx.getIOManager());
                    this.partSpillRunWriters[spillPartID].open();
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                }
                flush(this.partSpillRunWriters[spillPartID], GrouperFlushOption.FLUSH_FOR_GROUP_STATE, spillPartID);
            } else {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE, spillPartID);
            }
            spillFrameTupleAppender.reset(frameManager.getFrame(this.partitionOutputBuffers[spillPartID]), true);
            if (!spillFrameTupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new HyracksDataException("Failed to flush a tuple of a spilling partition");
            }
        }
    }

    private void flush(
            IFrameWriter writer,
            IGrouperFlushOption flushOption,
            int partitionIndex) throws HyracksDataException {

        this.outputFrameTupleAppender.reset(frameManager.getFrame(hashtableOutputBuffer), true);

        ByteBuffer bufToFlush = null;

        int workingFrameID = -1, prevFrameID = -1;

        IAggregatorDescriptor aggDesc;

        if (flushOption.getOutputState() == GroupOutputState.GROUP_STATE) {
            aggDesc = aggregator;
        } else if (flushOption.getOutputState() == GroupOutputState.RESULT_STATE) {
            aggDesc = merger;
        } else {
            throw new HyracksDataException("Cannot output " + GroupOutputState.RAW_STATE.name()
                    + " for flushing hybrid hash grouper");
        }

        if (this.partitionSpillingFlags[partitionIndex]) {
            workingFrameID = this.partitionOutputBuffers[partitionIndex];
        } else {
            workingFrameID = this.partitionBuffers[partitionIndex];
        }

        bufToFlush = frameManager.getFrame(workingFrameID);

        while (bufToFlush != null) {
            groupFrameTupleAccessor.reset(bufToFlush);
            int tupleCount = groupFrameTupleAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                outputTupleBuilder.reset();
                for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                    outputTupleBuilder.addField(groupFrameTupleAccessor, i, k);
                }
                aggDesc.outputFinalResult(outputTupleBuilder, groupFrameTupleAccessor, i, aggState);

                if (!this.outputFrameTupleAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                        outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                            outputFrameTupleAppender.getTupleCount());
                    if (flushOption == GrouperFlushOption.FLUSH_FOR_GROUP_STATE) {
                        debugOptionalIODumped++;
                    } else {
                        debugOptionalIOStreamed++;
                    }
                    if (isGenerateRuns) {
                        profileIOOutDisk++;
                    } else {
                        profileIOOutNetwork++;
                    }

                    profileOutFrames++;
                    profileOutRecords += outputFrameTupleAppender.getTupleCount();

                    FrameUtils.flushFrame(frameManager.getFrame(hashtableOutputBuffer), writer);

                    outputFrameTupleAppender.reset(frameManager.getFrame(hashtableOutputBuffer), true);
                    if (!outputFrameTupleAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException(
                                "Failed to dump a group from the hash table to a frame: possibly the size of the tuple is too large.");
                    }
                }
            }
            if (!this.partitionSpillingFlags[partitionIndex]) {
                prevFrameID = workingFrameID;
                workingFrameID = frameManager.getNextFrame(workingFrameID);
                if (workingFrameID >= 0) {
                    frameManager.recycleFrame(prevFrameID);
                    this.partitionBufferCounters[partitionIndex]--;
                    this.partitionBuffers[partitionIndex] = workingFrameID;
                    bufToFlush = frameManager.getFrame(workingFrameID);

                } else {
                    bufToFlush = null;
                }
            } else {
                bufToFlush = null;
            }
        }

        if (this.outputFrameTupleAppender.getTupleCount() > 0) {
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                    outputFrameTupleAppender.getTupleCount());
            if (flushOption == GrouperFlushOption.FLUSH_FOR_GROUP_STATE) {
                debugOptionalIODumped++;
            } else {
                debugOptionalIOStreamed++;
            }
            if (isGenerateRuns) {
                profileIOOutDisk++;
            } else {
                profileIOOutNetwork++;
            }

            profileOutFrames++;
            profileOutRecords += outputFrameTupleAppender.getTupleCount();

            FrameUtils.flushFrame(frameManager.getFrame(hashtableOutputBuffer), writer);
            outputFrameTupleAppender.reset(frameManager.getFrame(hashtableOutputBuffer), true);
        }

        // Mark the partition as spilled, if it has not been spilled before
        if (!this.partitionSpillingFlags[partitionIndex]) {
            this.partitionSpillingFlags[partitionIndex] = true;
            this.partitionOutputBuffers[partitionIndex] = prevFrameID;
            this.frameManager.resetFrame(this.partitionOutputBuffers[partitionIndex]);
            this.partitionBuffers[partitionIndex] = -1;
            this.partitionBuffers[partitionIndex] = 1;
            if (!this.partitionPinFlags[partitionIndex]) {
                this.unpinnedSpilledParts.add(partitionIndex);
            }

            // adjust the in-memory group/record statistics
            thresholdTotalGroupsInMem -= this.groupsInParts[partitionIndex];
            thresholdTotalRecordsInMem -= this.recordsInParts[partitionIndex];

            assert thresholdTotalGroupsInMem >= 0;
            assert thresholdTotalRecordsInMem >= 0;
        }
    }

    private boolean findMatch(
            FrameTupleAccessor inputTupleAccessor,
            int tupleIndex,
            int rawHashValue,
            int htSlotID) throws HyracksDataException {
        getHTSlotPointer(htSlotID);

        if (htLookupFrameIndex < 0) {
            return false;
        }

        // do bloom filter lookup, if bloom filter is enabled.
        if (this.isHashtableFull) {
            if (useMiniBloomFilter) {
                if (!lookupBloomFilter(rawHashValue, bloomFilterByte)) {
                    debugBloomFilterFail++;
                    return false;
                } else {
                    debugBloomFilterSucc++;
                }
            }
        }

        while (htLookupFrameIndex >= 0) {
            groupFrameTupleAccessor.reset(frameManager.getFrame(htLookupFrameIndex));
            if (!sameGroup(inputTupleAccessor, tupleIndex, groupFrameTupleAccessor, htLookupTupleIndex)) {
                int tupleEndOffset = groupFrameTupleAccessor.getTupleEndOffset(htLookupTupleIndex);
                htLookupFrameIndex = groupFrameTupleAccessor.getBuffer().getInt(
                        tupleEndOffset - (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE));
                htLookupTupleIndex = groupFrameTupleAccessor.getBuffer().getInt(tupleEndOffset - HT_TUPLE_REF_SIZE);
            } else {
                debugOptionalCPUCompareHit += debugTempCPUCounter;
                debugOptionalHashHits++;
                debugTempCPUCounter = 0;
                return true;
            }
        }
        debugOptionalCPUCompareHit += debugTempCPUCounter;
        debugOptionalHashHits++;
        debugTempCPUCounter = 0;
        // by default: no match is found
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

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#fail()
     */
    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#close()
     */
    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub
        dumpAndCleanDebugCounters();
    }

    @Override
    protected void flush(
            IFrameWriter writer,
            GrouperFlushOption flushOption) throws HyracksDataException {

    }

    @Override
    public void wrapup() throws HyracksDataException {
        // process spilled partitions
        for (int i = 0; i < numParts; i++) {
            if (this.partitionSpillingFlags[i]) {
                // simply flush the output buffer for the partition
                if (isGenerateRuns) {
                    // the buffer for this spilled partition is full
                    if (this.partSpillRunWriters[i] == null) {
                        this.partSpillRunWriters[i] = new RunFileWriter(
                                ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()),
                                ctx.getIOManager());
                        this.partSpillRunWriters[i].open();
                        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                    }
                    flush(this.partSpillRunWriters[i], GrouperFlushOption.FLUSH_FOR_RESULT_STATE, i);
                    runReaders.add(this.partSpillRunWriters[i].createReader());
                    this.partSpillRunWriters[i].close();
                    this.recordsInsertedForParts.add(this.recordsInParts[i]);
                    this.keysInsertedForParts.add(this.groupsInParts[i]);
                } else {
                    flush(outputWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE, i);
                }
            }
        }

        // process resident partitions
        for (int i = 0; i < numParts; i++) {
            if (!this.partitionSpillingFlags[i]) {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, i);
                this.recordsInResidentParts += this.recordsInParts[i];
                this.groupsInResidentParts += this.groupsInParts[i];
            }
        }

    }

    @Override
    public List<Long> getOutputRunSizeInRows() throws HyracksDataException {
        return recordsInsertedForParts;
    }

    @Override
    public long getRecordsCompletelyAggregated() {
        return recordsInResidentParts;
    }

    @Override
    public long getGroupsCompletelyAggregated() {
        return groupsInResidentParts;
    }

    @Override
    protected void dumpAndCleanDebugCounters() {

        this.debugCounters.updateRequiredCounter(RequiredCounters.CPU, debugRequiredCPU);
        this.debugCounters.updateRequiredCounter(RequiredCounters.IO_OUT_DISK, debugOptionalIODumped);

        this.debugCounters.updateRequiredCounter(RequiredCounters.IN_FRAMES, profileInFrames);
        this.debugCounters.updateRequiredCounter(RequiredCounters.IN_RECOEDS, profileInRecords);
        this.debugCounters.updateRequiredCounter(RequiredCounters.OUT_FRAMES, profileOutFrames);

        this.debugCounters.updateRequiredCounter(RequiredCounters.OUT_RECORDS, profileOutRecords);

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
        this.profileInRecords = 0;
        this.profileInFrames = 0;
        this.profileOutRecords = 0;
        this.profileOutFrames = 0;
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

    @Override
    public List<Long> getOutputGroupsInRows() throws HyracksDataException {
        return this.keysInsertedForParts;
    }
}
