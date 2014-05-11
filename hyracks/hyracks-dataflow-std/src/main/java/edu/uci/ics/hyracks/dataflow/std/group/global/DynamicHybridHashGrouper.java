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

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.group.FrameMemManager;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashFunctionFamilyFactoryAdapter;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashTableFrameTupleAppender;

public class DynamicHybridHashGrouper implements IFrameWriter {

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
    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IAggregatorDescriptorFactory aggregatorFactory,
            partialMergerFactory, finalMergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;

    /**
     * Input data statistics: total number of records and the expected
     * number of output records.
     */
    private final long inputRecordCount, outputGroupCount;

    /**
     * The size of the group state, used to properly size the hash table.
     */
    private final int groupStateSizeInBytes;

    /**
     * About the fuzziness of the hashing and partitioning
     */
    private final double fudgeFactor;

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
     * The flag for whether it is a map group-by (so complete aggregtion is not
     * needed)
     */
    private final boolean isMapGby;

    /**
     * The flag on whether the resident partition (i.e. in-memory hash table)
     * should be
     * partitioned and dynamically managed for better skew tolerance.
     */
    private final boolean isResidentPartitioned;

    /**
     * The number of hash table slots.
     */
    private int tableSize;

    /**
     * The number of spilled partitions.
     */
    private int numSpilledParts;

    /**
     * The index of the first page of each spilled partition.
     */
    private int[] spilledPartsBufferIndex;

    private boolean[] spilledPartsFlags;

    /**
     * The number of resident partitions, which is used to improve the skew
     * tolerance.
     */
    private int numResidentParts;

    /**
     * The index of the first page of each resident partition.
     */
    private int[] residentPartsBufferIndex;

    private boolean[] residentPartsFlags;

    /**
     * The frame manager to manage the buffers.
     */
    private FrameMemManager frameManager;

    /**
     * Buffer IDs for the hash table headers. Each element in this array is a
     * frame ID from the frameManager.
     */
    private int[] htHeaderFrameIds;

    /**
     * Buffer IDs for the hash table contents. The length of the array is equal
     * to the number of partitions in the resident partition, and each element
     * is the frame ID of the first frame in the list of frames for that
     * resident partition.
     */
    private int[] htContentFrameIds;

    /**
     * Output buffer IDs for the spilled partitions.
     */
    private int[] spilledPartsOutBufferIds;

    /**
     * The buffer for output resident partition
     */
    private int residentPartOutputBufferId;

    /**
     * The number of raw records and keys in each resident partition. These two
     * arrays are used to pick the resident partition with lowest absorption
     * ratio to spill.
     */
    private long[] recordsInResidentParts, keysInResidentParts;

    /**
     * The flag on whether the resident partition is decided by
     */
    private boolean isPartitionPredefined;

    /**
     * Hashing level random seed, so that group-by operations at different
     * levels could have different hash functions.
     */
    private final int hashLevelSeed;

    /**
     * Whether to use mini-bloomfilter to enhance the hashtable lookup
     */
    private final boolean useMiniBloomFilter;

    /**
     * The ratio between hash table slot count and the number of records that
     * can fit in the hasht able
     */
    private final double hashtableSlotRatio;

    private int[] keysInPartialResult;

    private IBinaryComparator[] comparators;
    private ITuplePartitionComputer tuplePartitionComputer;
    private IAggregatorDescriptor aggregator, partialMerger, finalMerger;
    private IBinaryHashFunctionFactory[] hashFunctionFactories;

    /**
     * Hash function level variable, used to indicate different hash levels (so
     * that different hash functions can be used)
     */
    private int hashLevelSeedVariable;

    /**
     * Frame accessors: inputFrameTupleAccessor is for the input frame, and
     * groupFrameTupleAccessor is for the group state.
     */
    private FrameTupleAccessor inputFrameTupleAccessor,
            groupFrameTupleAccessor;

    /**
     * Tuple builders: groupTupleBuild is for the group state in memory, and
     * outputTupleBuilder is for the flushing state out of the memory.
     */
    private ArrayTupleBuilder groupTupleBuilder, outputTupleBuilder;

    private HashTableFrameTupleAppender hashtableFrameTupleAppender;

    private FrameTupleAppender spillFrameTupleAppender,
            outputFrameTupleAppender;

    /**
     * The number of hash table slots each frame can contain
     */
    private int htSlotsPerFrame;

    /**
     * The hashtable lookup index for frame and tuple
     */
    private int htLookupFrameIndex = -1, htLookupTupleIndex = -1;

    /**
     * The bloomfilter lookup cache to store the bloomfilter byte found.
     */
    private byte bloomFilterByte = (byte) -1;

    /**
     * The max hash key for resident partition (so the resident partition is in
     * the range of [0, maxResidentHashkey]).
     */
    private int maxResidentHashkey;

    /**
     * The number of hash keys in a spilled partition.
     */
    private int spilledPartHashkeyRange;

    /**
     * Is the hash table (for resident partition) full
     */
    private boolean isHashtableFull = false;

    public DynamicHybridHashGrouper(IHyracksTaskContext ctx, int[] keyFields,
            int[] decorFields, int framesLimit, long inputRecordCount,
            long outputGroupCount, int groupStateSizeInBytes,
            double fudgeFactor, double htSlotRatio,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies,
            IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory,
            RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int hashLevelSeed,
            IFrameWriter outputWriter, boolean isMapGroupBy,
            boolean isResidentPartitioned, boolean useMiniBloomFilter)
            throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.frameSize = ctx.getFrameSize();
        this.framesLimit = framesLimit;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.hashFunctionFamilies = hashFunctionFamilies;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;
        this.hashLevelSeed = hashLevelSeed;
        this.outputWriter = outputWriter;

        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;
        this.inputRecordCount = inputRecordCount;
        this.outputGroupCount = outputGroupCount;
        this.groupStateSizeInBytes = groupStateSizeInBytes;
        this.fudgeFactor = fudgeFactor;
        this.hashtableSlotRatio = htSlotRatio;

        this.isMapGby = isMapGroupBy;
        this.isResidentPartitioned = isResidentPartitioned;
        this.useMiniBloomFilter = useMiniBloomFilter;

    }

    /**
     * This method decides the proper memory layout for the resident and spilled
     * partitions, according to the input parameters. The basic algorithm to
     * follow here is:<br/>
     * 1. If isMapGby is true, only one spilled partition is needed, as we
     * assume that the output buffers for partitioning are allocated by system
     * so the algorithm does not need to do this. Also, the resident partition
     * will not be pinned in memory so it finally could be spilled, if the
     * absorption ratio is ??? <br/>
     * 1.a If resident partition should be partitioned for better skew
     * tolerance, then the number of partitions in the resident partition should
     * be at least 2 (so around half of the resident partition could be spilled
     * and renewed) but less than m/2 (so at least two frames should be
     * assigned) to each resident partition. <br/>
     * 1.b Otherwise, there will be only one partition in the resident
     * partition, then all memory but one will be used for the in-memory
     * hashtable
     * 2. Otherwise, P spilled partitions are needed. P is computed based on the
     * hybrid-hash formula.
     * 2.a If resident partition should be partitioned for better skew
     * tolerance, still the number of partitions in the resident partition
     * should be at least p, where m is the number of frames for
     * the hash table contents.
     * 2.b Otherwise, all the memory allocated to the resident partition will be
     * considered as a single partition, which is similar to the
     * pre-partitioning algorithm.
     * 
     * @throws HyracksDataException
     */
    private void initMemoryLayout() throws HyracksDataException {

        // initialize the frame manager
        this.frameManager = new FrameMemManager(framesLimit, ctx);

        // allocate the resident partition output buffer
        this.residentPartOutputBufferId = frameManager.allocateFrame();

        // frames for the hashtable: total and headers only
        int framesForHashtable, framesForHashtableHeaders;

        if (isMapGby) {
            // no spilled partition is needed
            this.numSpilledParts = 0;
            // use M-1 frames for the hash table
            framesForHashtable = framesLimit - 1;

        } else {
            // compute the number of partitions, using hybrid hash algorithm
            this.numSpilledParts = computeHybridHashSpilledPartitions(
                    framesLimit, frameSize, outputGroupCount,
                    groupStateSizeInBytes, fudgeFactor);
            if (!isResidentPartitioned) {
                /**
                 * If the resident partition is not partitions, enough
                 * frames should be reserved so that the spilled partitions
                 * can be properly spilled
                 */
                framesForHashtable = framesLimit - 1 - numSpilledParts;
            } else {
                // Use all memory for the hash table
                framesForHashtable = framesLimit - 1;
            }

        }

        this.tableSize = computeHashtableSlots(framesForHashtable, frameSize,
                groupStateSizeInBytes, hashtableSlotRatio, HT_FRAME_REF_SIZE,
                HT_TUPLE_REF_SIZE, useMiniBloomFilter,
                HT_MINI_BLOOM_FILTER_SIZE);
        framesForHashtableHeaders = getHeaderFrameCountForHashtable(
                this.tableSize, frameSize, HT_FRAME_REF_SIZE,
                HT_TUPLE_REF_SIZE, useMiniBloomFilter,
                HT_MINI_BLOOM_FILTER_SIZE);

        if (isResidentPartitioned) {
            this.numResidentParts = getResidentPartitions(framesForHashtable
                    - framesForHashtableHeaders, numSpilledParts);
        } else {
            this.numResidentParts = 1;
        }

        /**
         * Compute the number of slots per frame for the hash table
         */
        this.htSlotsPerFrame = frameSize
                / (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE
                        : 0));

        // create the hash table structures
        initializeHashtable(framesForHashtableHeaders, framesForHashtable
                - framesForHashtableHeaders);
    }

    /**
     * Compute the inner partitions for the resident partition.
     * 
     * @param framesForHTContents
     * @param numSpilledPartitions
     * @return
     */
    private int getResidentPartitions(int framesForHTContents,
            int numSpilledPartitions) {
        return (int) Math.max(numSpilledPartitions, framesForHTContents
                / MIN_FRAMES_PER_RESIDENT_PART);
    }

    /**
     * Compute the number of hybrid hash spilled partitions.
     * 
     * @param framesLimit
     * @param frameSize
     * @param outputRecordCount
     * @param recordSize
     * @param fudgeFactor
     * @return
     */
    public static int computeHybridHashSpilledPartitions(int framesLimit,
            int frameSize, long outputRecordCount, int recordSize,
            double fudgeFactor) {
        return (int) Math.ceil((Math.ceil(outputRecordCount * recordSize
                * fudgeFactor / frameSize) - (framesLimit - 1))
                / (framesLimit - 2));
    }

    /**
     * Compute the hash table slots for the given number of frames. The goal is
     * to divide the frames into two parts, one for the hash table headers and
     * the other is for the hash table contents, so that the number of slots is
     * htSlotRatio times of the number of table entries.
     * 
     * @param framesForHashtable
     * @return
     */
    public static int computeHashtableSlots(int framesForHashtable,
            int frameSize, int recordSize, double htSlotRatio,
            int bytesForFrameReference, int bytesForTupleReference,
            boolean useMiniBloomfilter, int bytesForMiniBloomfilter) {
        return (int) Math
                .floor(framesForHashtable
                        * frameSize
                        / (((useMiniBloomfilter ? bytesForMiniBloomfilter : 0)
                                + bytesForFrameReference + bytesForTupleReference))
                        + (recordSize + bytesForFrameReference + bytesForTupleReference)
                        / htSlotRatio);
    }

    /**
     * Compute the number of header pages for the hash table, given the number
     * of slots.
     * 
     * @param hashtableSlots
     * @param frameSize
     * @param bytesForFrameRef
     * @param bytesForTupleRef
     * @param useMiniBloomfilter
     * @param bytesForMiniBloomfilter
     * @return
     */
    public static int getHeaderFrameCountForHashtable(int hashtableSlots,
            int frameSize, int bytesForFrameRef, int bytesForTupleRef,
            boolean useMiniBloomfilter, int bytesForMiniBloomfilter) {
        return (int) Math.ceil(hashtableSlots
                * ((useMiniBloomfilter ? bytesForMiniBloomfilter : 0)
                        + bytesForFrameRef + bytesForTupleRef) / frameSize);
    }

    /**
     * Initialize the header pages for the hash table.
     * 
     * @param hashtableHeaderFrameCount
     * @param hashtableContentFrameCount
     * @throws HyracksDataException
     */
    private void initializeHashtable(int hashtableHeaderFrameCount,
            int hashtableContentFrameCount) throws HyracksDataException {
        if (hashtableHeaderFrameCount < 0 || hashtableContentFrameCount < 0) {
            throw new HyracksDataException("Hash table header frame ("
                    + hashtableHeaderFrameCount + ") and content frames ("
                    + hashtableContentFrameCount + ") cannot be negative.");
        }
        this.htHeaderFrameIds = new int[hashtableHeaderFrameCount];
        for (int i = 0; i < hashtableHeaderFrameCount; i++) {
            this.htHeaderFrameIds[i] = frameManager.allocateFrame();
            if (this.htHeaderFrameIds[i] == -1) {
                throw new HyracksDataException("Not enough memory for "
                        + hashtableHeaderFrameCount
                        + " header pages of a hash table.");
            }
            frameManager.resetFrame(htHeaderFrameIds[i]);
        }
        this.htContentFrameIds = new int[hashtableContentFrameCount];
        for (int i = 0; i < hashtableContentFrameCount; i++) {
            this.htContentFrameIds[i] = -1;
        }

        // update buffers for resident partitions
        this.residentPartsBufferIndex = new int[this.numResidentParts];

        // initialize buffers for spilled partitions
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
            this.comparators[i] = this.comparatorFactories[i]
                    .createBinaryComparator();
        }

        this.hashFunctionFactories = new IBinaryHashFunctionFactory[this.hashFunctionFamilies.length];
        for (int i = 0; i < hashFunctionFactories.length; i++) {
            hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter
                    .getFunctionFactoryFromFunctionFamily(
                            this.hashFunctionFamilies[i], hashLevelSeed
                                    + hashLevelSeedVariable);
        }

        this.tuplePartitionComputer = new FieldHashPartitionComputerFactory(
                keyFields, hashFunctionFactories).createPartitioner();

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDesc,
                outRecordDesc, keyFields, keysInPartialResult, null);
        this.partialMerger = partialMergerFactory.createAggregator(ctx,
                outRecordDesc, outRecordDesc, keysInPartialResult,
                keysInPartialResult, null);
        this.finalMerger = finalMergerFactory.createAggregator(ctx,
                outRecordDesc, outRecordDesc, keysInPartialResult,
                keysInPartialResult, null);

        this.inputFrameTupleAccessor = new FrameTupleAccessor(frameSize,
                inRecordDesc);
        this.groupFrameTupleAccessor = new FrameTupleAccessor(frameSize,
                outRecordDesc);

        this.groupTupleBuilder = new ArrayTupleBuilder(
                outRecordDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(
                outRecordDesc.getFieldCount());

        this.hashtableFrameTupleAppender = new HashTableFrameTupleAppender(
                frameSize, HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE);

        this.spillFrameTupleAppender = new FrameTupleAppender(frameSize);
        this.outputFrameTupleAppender = new FrameTupleAppender(frameSize);

        this.maxResidentHashkey = MAX_RAW_HASHKEY
                / (framesLimit - numSpilledParts + numSpilledParts
                        * framesLimit) * (framesLimit - numSpilledParts);
        if (numSpilledParts == 0) {
            this.maxResidentHashkey = MAX_RAW_HASHKEY;
        }
        this.spilledPartHashkeyRange = (Integer.MAX_VALUE - maxResidentHashkey)
                / (framesLimit - numSpilledParts + numSpilledParts
                        * framesLimit) * framesLimit + 1;
        if (numSpilledParts == 0) {
            this.spilledPartHashkeyRange = 0;
        }

        // Initialize the memory layout
        initMemoryLayout();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        this.inputFrameTupleAccessor.reset(buffer);
        int tupleCount = this.inputFrameTupleAccessor.getTupleCount();

        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
            int rawHashValue = tuplePartitionComputer.partition(
                    inputFrameTupleAccessor, tupleIndex, MAX_RAW_HASHKEY);
            int htSlotID = rawHashValue % this.tableSize;

        }
    }

    /**
     * Get the hash partition id for the given raw hash key. The input hash key
     * should be in the scope of [0, MAX_INT]. And the returned value should be
     * 0 (for resident partition) or [1, P] (for spilled partitions).
     * 
     * @param hashkey
     * @return
     * @throws HyracksDataException
     */
    private int getPartitionID(int rawHashkey) throws HyracksDataException {
        int partitionID = -1;
        if (isPartitionPredefined) {
            // use hash-based partition
            if (rawHashkey <= maxResidentHashkey) {
                partitionID = 0;
            } else {
                partitionID = (rawHashkey - maxResidentHashkey)
                        / spilledPartHashkeyRange + 1;
            }
        } else {
            // for non-predefined case, always assume the record is in resident
            // partition, and later if there is no match is found, it is then
            // assigned to a spilled partition
            partitionID = 0;
        }
        if (partitionID == 0) {
            int residentSubpartID = rawHashkey % numResidentParts;
            if (residentPartsFlags[residentSubpartID]) {
                partitionID = rawHashkey % numSpilledParts + 1;
            }
        }
        return partitionID;
    }

    /**
     * Pick a resident subpartition to spill. If none of the resident
     * sub-partition can be spilled, return -1;
     * 
     * @return
     */
    private int pickResidentSubPartToSpill() {
        return -1;
    }

    private boolean findMatch(FrameTupleAccessor inputTupleAccessor,
            int tupleIndex, int rawHashValue, int htSlotID)
            throws HyracksDataException {
        
        // by default: no match is found
        return false;
    }

    /**
     * Get the bloomfilter byte (if needed), tuple index and frame index from
     * the hash table for the given slot. Return -1 if the slot is empty.
     * 
     * @param htSlotID
     * @throws HyracksDataException
     */
    private void getHTSlotPointers(int htSlotID) throws HyracksDataException {
        int slotFrameIndex = htSlotID / htSlotsPerFrame;
        int slotTupleOffset = htSlotID
                % htSlotsPerFrame
                * (HT_FRAME_REF_SIZE + HT_TUPLE_REF_SIZE + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE
                        : 0));
        ByteBuffer targetHeaderPage = frameManager.getFrame(slotFrameIndex);
        if (targetHeaderPage == null) {
            bloomFilterByte = (byte) -1;
            htLookupFrameIndex = -1;
            htLookupTupleIndex = -1;
        } else {
            if (useMiniBloomFilter) {
                bloomFilterByte = targetHeaderPage.get(slotTupleOffset);
            }
            htLookupFrameIndex = targetHeaderPage.getInt(slotTupleOffset
                    + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0));
            htLookupTupleIndex = targetHeaderPage.getInt(slotTupleOffset
                    + (useMiniBloomFilter ? HT_MINI_BLOOM_FILTER_SIZE : 0)
                    + HT_FRAME_REF_SIZE);
        }
    }

    /**
     * Get the resident partition containing the given hash key. Return -1 if
     * the given hash key should be spilled (for both spilled partitions and the
     * spilled resident partitions)
     * 
     * @param rawHashkey
     * @return
     * @throws HyracksDataException
     */
    private int getResidentPartitionID(int rawHashkey)
            throws HyracksDataException {

        return -1;
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

}
