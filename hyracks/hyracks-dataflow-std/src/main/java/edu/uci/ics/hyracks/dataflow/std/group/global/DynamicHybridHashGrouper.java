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
     * The total number of usable frames, as the memory resource.
     */
    private final int framesLimit;

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

    private final int[] partitionOutputBuffers;

    private final boolean[] partitionSpillingFlags;

    private final boolean pinHighAbsorptionPartitions;
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
    private long[] recordsInParts, keysInParts;

    /**
     * Whether to use mini-bloomfilter to enhance the hashtable lookup
     */
    private final boolean useMiniBloomFilter;

    private int[] keysInPartialResult;

    private IBinaryComparator[] comparators;
    private ITuplePartitionComputer tuplePartitionComputer;
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

    private final RunFileWriter[] partSpillRunWriters;

    private static final Logger LOGGER = Logger.getLogger(DynamicHybridHashGrouper.class.getSimpleName());

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
            boolean pinHighAbsorptionParts,
            boolean alwaysPinLastPart) throws HyracksDataException {

        super(ctx, keyFields, decorFields, framesLimit, aggregatorFactory, mergerFactory, inRecordDescriptor,
                outRecordDescriptor, enableHistorgram, outputWriter, isGenerateRuns);

        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.frameSize = ctx.getFrameSize();
        this.framesLimit = framesLimit;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;
        this.outputWriter = outputWriter;

        this.tableSize = tableSize;

        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;
        this.pinHighAbsorptionPartitions = pinHighAbsorptionParts;
        this.pinLastPartitionAlways = alwaysPinLastPart;

        this.useMiniBloomFilter = useMiniBloomFilter;

        this.numParts = numParts;
        this.partitionBuffers = new int[this.numParts];
        this.partitionSpillingFlags = new boolean[this.numParts];
        this.partitionPinFlags = new boolean[this.numParts];
        this.partitionOutputBuffers = new int[this.numParts];
        this.recordsInParts = new long[this.numParts];
        this.keysInParts = new long[this.numParts];
        for (int i = 0; i < this.numParts; i++) {
            this.partitionSpillingFlags[i] = false;
            this.partitionPinFlags[i] = false;
            this.partitionBuffers[i] = -1;
            this.partitionOutputBuffers[i] = -1;
        }

        this.isGenerateRuns = isGenerateRuns;
        this.partSpillRunWriters = new RunFileWriter[this.numParts];

        this.frameManager = new FrameMemManager(framesLimit, ctx);
        this.hashtableOutputBuffer = frameManager.allocateFrame();
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

        this.tuplePartitionComputer = new FieldHashPartitionComputerFactory(keyFields, hashFunctionFactories)
                .createPartitioner();

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
     * edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(
            ByteBuffer buffer) throws HyracksDataException {
        this.inputFrameTupleAccessor.reset(buffer);
        int tupleCount = this.inputFrameTupleAccessor.getTupleCount();

        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
            int rawHashValue = tuplePartitionComputer.partition(inputFrameTupleAccessor, tupleIndex, MAX_RAW_HASHKEY);
            int htSlotID = rawHashValue % this.tableSize;
            int partID = htSlotID % this.numParts;
            if (this.partitionSpillingFlags[partID]) {
                // partition is spilled
                int spillPartID = partID;
                if (this.partitionPinFlags[partID]) {
                    // partition has been pinned, then it will be partitioned
                    // into other non-pinned spilled partitions
                    spillPartID = getUnpinnedSpilledPartIDForSpilling(partID);
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
                this.recordsInParts[partID]++;
                this.keysInParts[partID]++;
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
                } else {
                    // haven't found a match: either spill (for pinned part) or
                    // add a new entry (for unpinned part)
                    this.groupTupleBuilder.reset();

                    for (int i : keyFields) {
                        groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                    }

                    for (int i : decorFields) {
                        groupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                    }

                    aggregator.init(groupTupleBuilder, inputFrameTupleAccessor, tupleIndex, aggState);

                    if (this.partitionPinFlags[partID]) {
                        // partition is pinned: need to flush this record
                        int unpinnedPartPicked = getUnpinnedSpilledPartIDForSpilling(partID);
                        spillGroup(groupTupleBuilder, unpinnedPartPicked);
                        this.recordsInParts[unpinnedPartPicked]++;
                        this.keysInParts[unpinnedPartPicked]++;
                    } else {
                        // try to insert a new entry
                        getHTSlotPointer(htSlotID);
                        if (this.partitionBuffers[partID] < 0) {
                            this.partitionBuffers[partID] = frameManager.allocateFrame();
                            if (this.partitionBuffers[partID] < 0) {
                                throw new HyracksDataException("Failed to allocate a frame for partition " + partID);
                            }
                        }
                        hashtableFrameTupleAppender.reset(frameManager.getFrame(this.partitionBuffers[partID]), false);
                        if (!hashtableFrameTupleAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                                groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize(), htLookupFrameIndex,
                                htLookupTupleIndex)) {

                            if (!allocateBufferForPart(partID)) {
                                // haven't got extra buffer for the partition
                                if (this.partitionSpillingFlags[partID]) {
                                    spillGroup(groupTupleBuilder, partID);
                                    this.recordsInParts[partID]++;
                                    this.keysInParts[partID]++;
                                } else {
                                    // must be the last partition pinned
                                    if (!this.partitionPinFlags[partID]) {
                                        throw new HyracksDataException(
                                                "Should not reach: partition is not pinned, but no extra frame can be allocated for it, nor it can be spilled");
                                    } else {
                                        int unpinnedPartPicked = getUnpinnedSpilledPartIDForSpilling(partID);
                                        spillGroup(groupTupleBuilder, unpinnedPartPicked);
                                        this.recordsInParts[unpinnedPartPicked]++;
                                        this.keysInParts[unpinnedPartPicked]++;
                                    }
                                }
                            } else {

                                hashtableFrameTupleAppender.reset(frameManager.getFrame(this.partitionBuffers[partID]),
                                        true);
                                if (!hashtableFrameTupleAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                                        groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize(),
                                        htLookupFrameIndex, htLookupTupleIndex)) {
                                    throw new HyracksDataException(
                                            "Failed to insert a group into the hash table: the record is too large.");
                                }
                                this.recordsInParts[partID]++;
                                this.keysInParts[partID]++;
                            }
                        }
                        if (useMiniBloomFilter) {
                            bloomFilterByte = insertIntoBloomFilter(rawHashValue, bloomFilterByte,
                                    (htLookupFrameIndex < 0));
                        }

                        // reset the header reference
                        setHTSlotPointer(htSlotID, bloomFilterByte, this.partitionBuffers[partID],
                                hashtableFrameTupleAppender.getTupleCount() - 1);
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
                if (this.partSpillRunWriters[partIDToSpill] == null) {
                    this.partSpillRunWriters[partIDToSpill] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()), ctx.getIOManager());
                    this.partSpillRunWriters[partIDToSpill].open();
                }
                flush(this.partSpillRunWriters[partIDToSpill], GrouperFlushOption.FLUSH_FOR_GROUP_STATE, partIDToSpill);
                if (partIDToSpill != partID) {
                    newFrameID = frameManager.allocateFrame();
                } else {
                    return false;
                }
            } else {
                break;
            }
        }

        if (newFrameID >= 0) {
            frameManager.setNextFrame(newFrameID, this.partitionBuffers[partID]);
            this.partitionBuffers[partID] = newFrameID;
            return true;
        } else {
            return false;
        }
    }

    private int pickPartitionToSpill() {
        int partIDToSpill = -1;

        int partsInMem = 0;
        double minAbsorptionRatio = Integer.MAX_VALUE;

        for (int i = 0; i < this.numParts; i++) {
            if (this.partitionSpillingFlags[i]) {
                continue;
            }
            partsInMem++;
            double currentAbsorptionRatio = (this.keysInParts[i] == 0) ? 0 : this.recordsInParts[i]
                    / this.keysInParts[i];
            if (currentAbsorptionRatio < minAbsorptionRatio) {
                minAbsorptionRatio = currentAbsorptionRatio;
                partIDToSpill = i;
            }
        }

        if (partsInMem == 1 && this.pinLastPartitionAlways) {
            // Only one partition is left: pin this partition, and use the hash table output buffer for it
            this.partitionPinFlags[partIDToSpill] = true;
            this.partitionOutputBuffers[partIDToSpill] = hashtableOutputBuffer;
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
            // right now at most one partition can be pinned
            int pinnedParts = 0;
            for (int i = 0; i < this.numParts; i++) {
                if (this.partitionPinFlags[i]) {
                    pinnedParts++;
                }
            }
            if (pinnedParts > 1) {
                throw new HyracksDataException("More than one partition is pinned!");
            } else {
                return partID;
            }
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
            FrameUtils.flushFrame(frameManager.getFrame(hashtableOutputBuffer), writer);
            outputFrameTupleAppender.reset(frameManager.getFrame(hashtableOutputBuffer), true);
        }

        if (!this.partitionSpillingFlags[partitionIndex]) {
            this.partitionSpillingFlags[partitionIndex] = true;
            this.partitionOutputBuffers[partitionIndex] = prevFrameID;
            this.partitionBuffers[partitionIndex] = -1;
            if (!this.partitionPinFlags[partitionIndex]) {
                this.unpinnedSpilledParts.add(partitionIndex);
            }
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
        if (useMiniBloomFilter) {
            if (!lookupBloomFilter(rawHashValue, bloomFilterByte)) {
                return false;
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
                return true;
            }
        }

        // by default: no match is found
        return false;
    }

    protected boolean sameGroup(
            FrameTupleAccessor a1,
            int t1Idx,
            FrameTupleAccessor a2,
            int t2Idx) {
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

    }

    @Override
    protected void flush(
            IFrameWriter writer,
            GrouperFlushOption flushOption) throws HyracksDataException {

    }

    @Override
    public void wrapup() throws HyracksDataException {
        for (int i = 0; i < numParts; i++) {
            if (this.partitionSpillingFlags[i]) {
                // simply flush the output buffer for the partition
                if (!this.partitionPinFlags[i]) {
                    if (this.partSpillRunWriters[i] == null) {
                        this.partSpillRunWriters[i] = new RunFileWriter(
                                ctx.createManagedWorkspaceFile(HybridHashGrouper.class.getSimpleName()),
                                ctx.getIOManager());
                        this.partSpillRunWriters[i].open();
                    }
                    flush(this.partSpillRunWriters[i], GrouperFlushOption.FLUSH_FOR_GROUP_STATE, i);
                }
            } else {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE, i);
            }
            if (this.partSpillRunWriters[i] != null) {
                runReaders.add(this.partSpillRunWriters[i].createReader());
                this.partSpillRunWriters[i].close();
            }
        }
    }

    @Override
    protected void dumpAndCleanDebugCounters() {
        // TODO Auto-generated method stub

    }

}
