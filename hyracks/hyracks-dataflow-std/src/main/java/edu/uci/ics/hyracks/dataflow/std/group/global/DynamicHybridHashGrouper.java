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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.group.FrameMemManager;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class DynamicHybridHashGrouper implements IFrameWriter {

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
     * The number of resident partitions, which is used to improve the skew
     * tolerance.
     */
    private int numResidentParts;

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
     * levels
     * could have different hash functions.
     */
    private final int hashLevelSeed;

    public DynamicHybridHashGrouper(IHyracksTaskContext ctx, int[] keyFields,
            int[] decorFields, int framesLimit, long inputRecordCount,
            long outputGroupCount, int groupStateSizeInBytes,
            double fudgeFactor,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies,
            IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory,
            RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int hashLevelSeed,
            IFrameWriter outputWriter, boolean isMapGroupBy,
            boolean isResidentPartitioned) throws HyracksDataException {
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

        this.isMapGby = isMapGroupBy;
        this.isResidentPartitioned = isResidentPartitioned;

        // Initialize the memory layout
        initMemoryLayout();
    }

    /**
     * This method decides the proper memory layout for the resident and spilled
     * partitions, according to the input parameters. The basic algorithm to
     * follow here is:<br/>
     * 1. If isMapGby is true, only one spilled partition is needed, as we
     * assume that the output buffer for partitioning are allocated by system so
     * the algorithm does not need to do this. Also, the resident partition will
     * not be pinned in memory so it finally could be spilled, if the absorption
     * ratio is ??? <br/>
     * 1.a If resident partition should be partitioned for better skew
     * tolerance, then the number of partitions in the resident partition should
     * be at least 2 (so around half of the resident partition could be spilled
     * and renewed) but less than m/2 (so at least two frames should be
     * assigned) to each resident partition. <br/>
     * 1.b Otherwise, there will be only one partition in the resident partition
     * 
     * @throws HyracksDataException
     */
    private void initMemoryLayout() throws HyracksDataException {

    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameWriter#open()
     */
    @Override
    public void open() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // TODO Auto-generated method stub

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
