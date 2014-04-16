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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashFunctionFamilyFactoryAdapter;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.AbstractHistogramPushBasedGrouper;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.HashGroupSortMergeGrouper;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.HashGrouper;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.HybridHashGrouper;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.PreCluster;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.RecursiveHybridHashGrouper;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.SortGroupMergeGrouper;
import edu.uci.ics.hyracks.dataflow.std.group.global.groupers.SortGrouper;

/**
 * This class is the hyracks operator for local group-by operation. It is implemented so that the actual
 * group-by algorithm can be picked during the runtime instead of only configurable at the compilation time.
 * <p/>
 * To initialize a local group operator, the following input parameters should be specified:<br/>
 * - group-by condition (keyFields) and the corrsponding comparators (comparatorFactories). <br/>
 * - group-by aggregation functions (aggregatorFactory, partialMergerFactory, and finalMergerFactory). Note that here
 * the three aggregation functions are used for different state transitions: aggregatorFactory for (raw ->
 * intermediate), partialMergerFactory (intermediate -> intermediate), and finalMergerFactory (intermediate -> final).<br/>
 * - (estimated) statistics about the input and output.<br/>
 * - assigned memory, represented as the number of frames (framesLimit). <br/>
 * - hashing schema, including the hash function (hashFamilies), random seed for hash function (levelSeed), hash table
 * slots count (tableSize) and fudge factor (fudgeFactor).<br/>
 * - sorting helper (firstNormalizerFactory).
 * <p/>
 * <b>About the aggregation states:</b>
 * <p/>
 * The aggregation states describe the states maintained for the aggregation results. We consider the following three
 * states:<br/>
 * - <b>Raw state</b>: representing the state of the raw input data.<br/>
 * - <b>Intermediate state</b>: representing the state when the aggregation result is in memory for accumulating.<br/>
 * - <b>Final state</b>: representing the state when the aggregation result is ready to be outputted.
 * <p/>
 * Take <b>AVG</b> as an example, if we want to compute the average of an integer field, the corresponding states are:<br/>
 * - <b>Raw state</b>: the integer field from the raw input data.<br/>
 * - <b>Intermediate state</b>: an integer sum value and a count value, maintained in memory.<br/>
 * - <b>Final state</b>: the average value computed by dividing the sum value by the count value.
 * <p/>
 */
public class LocalGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int framesLimit, levelSeed, tableSize;

    private final int[] keyFields, decorFields;

    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;

    private final IBinaryComparatorFactory[] comparatorFactories;

    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IBinaryHashFunctionFamily[] hashFamilies;

    private final GroupAlgorithms algorithm;

    private final boolean isInputGlobalHashPartitioned;

    private final int groupStateSizeInBytes;
    private final double fudgeFactor;

    /**
     * Three group-by output states
     */
    public enum GroupOutputState {
        /**
         * raw input data
         */
        RAW_STATE, 
        /**
         * Partial aggregated data (need merging)     
         */
        GROUP_STATE, 
        /**
         * Completely aggregated data (ready for output)
         */
        RESULT_STATE
    }
    
    public enum GroupAlgorithms {
        SORT_GROUP,
        SORT_GROUP_MERGE_GROUP,
        HASH_GROUP,
        HASH_GROUP_SORT_MERGE_GROUP,
        SIMPLE_HYBRID_HASH,
        RECURSIVE_HYBRID_HASH,
        PRECLUSTER;

        public boolean canBeTerminal() throws HyracksDataException {
            switch (this) {
                case SORT_GROUP:
                case HASH_GROUP:
                case SIMPLE_HYBRID_HASH:
                    return false;
                case SORT_GROUP_MERGE_GROUP:
                case HASH_GROUP_SORT_MERGE_GROUP:
                case RECURSIVE_HYBRID_HASH:
                case PRECLUSTER:
                    return true;
            }
            throw new HyracksDataException("Unsupported grouper: " + this.name());
        }
    }

    public LocalGroupOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] keyFields, int[] decorFields,
            int framesLimit, int tableSize, int groupStateSizeInBytes,
            double fudgeFactor, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFamilies, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor outRecDesc, GroupAlgorithms algorithm,
            int levelSeed, boolean isInputGlobalHashPartitioned) throws HyracksDataException {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        if (framesLimit <= 3) {
            throw new HyracksDataException("Not enough memory assigned for " + this.displayName
                    + ": at least 3 frames are necessary but just " + framesLimit + " available.");
        }
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.levelSeed = levelSeed;
        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.hashFamilies = hashFamilies;
        recordDescriptors[0] = outRecDesc;
        this.algorithm = algorithm;

        this.groupStateSizeInBytes = groupStateSizeInBytes;
        this.fudgeFactor = fudgeFactor;

        this.isInputGlobalHashPartitioned = isInputGlobalHashPartitioned;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

        final RecordDescriptor outRecDesc = recordDescriptors[0];

        final IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[this.hashFamilies.length];
        for (int i = 0; i < hashFunctionFactories.length; i++) {
            hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                    this.hashFamilies[i], levelSeed);
        }

        // compute the number of records and groups in this partition
        final long recordsInPartition = inputPartitionDesc.getRecordCountInPartition(partition);

        final long groupsInPartitions = inputPartitionDesc.getKeyCountInPartition(partition);

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private IFrameWriter grouper = null;

            private long debugInputFrameCount = 0;

            private long debugElapsedTime = System.currentTimeMillis();

            @Override
            public void open() throws HyracksDataException {
                switch (algorithm) {
                    case SORT_GROUP:
                        grouper = new SortGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, firstNormalizerFactory, comparatorFactories,
                                writer, false);
                        break;
                    case HASH_GROUP:
                        grouper = new HashGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, false, writer, false, tableSize,
                                comparatorFactories, hashFunctionFactories, firstNormalizerFactory);
                        break;
                    case HASH_GROUP_SORT_MERGE_GROUP:
                        grouper = new HashGroupSortMergeGrouper(ctx, keyFields, decorFields, framesLimit, tableSize,
                                firstNormalizerFactory, comparatorFactories, hashFunctionFactories, aggregatorFactory,
                                partialMergerFactory, finalMergerFactory, inRecDesc, outRecDesc, writer);
                        break;
                    case SIMPLE_HYBRID_HASH:
                        grouper = new HybridHashGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, false, writer, false, tableSize,
                                comparatorFactories, hashFunctionFactories, 1, true, isInputGlobalHashPartitioned);
                        break;
                    case RECURSIVE_HYBRID_HASH:
                        grouper = new RecursiveHybridHashGrouper(ctx, keyFields, decorFields, framesLimit, tableSize,
                                recordsInPartition, groupsInPartitions, groupStateSizeInBytes, fudgeFactor,
                                firstNormalizerFactory, comparatorFactories, hashFamilies, aggregatorFactory,
                                partialMergerFactory, finalMergerFactory, inRecDesc, outRecDesc, 0, writer,
                                isInputGlobalHashPartitioned);
                        break;
                    case PRECLUSTER:
                        grouper = new PreCluster(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, comparatorFactories, writer);
                        break;
                    case SORT_GROUP_MERGE_GROUP:
                    default:
                        grouper = new SortGroupMergeGrouper(ctx, keyFields, decorFields, framesLimit,
                                firstNormalizerFactory, comparatorFactories, aggregatorFactory, partialMergerFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, writer);
                        break;

                }
                writer.open();
                grouper.open();
            }

            /**
             * Note that here it if possible to pick the group-by algorithm dynamically during the
             * runtime. By collecting the statistics of the input data through the histogram from the
             * {@link AbstractHistogramPushBasedGrouper}, the grouper can be changed to use the proper algorithm.
             */
            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                debugInputFrameCount++;
                grouper.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                switch (algorithm) {
                    case SORT_GROUP:
                    case HASH_GROUP:
                    case SIMPLE_HYBRID_HASH:
                    case PRECLUSTER:
                        ((AbstractHistogramPushBasedGrouper) grouper).wrapup();
                        break;
                    default:
                        break;
                }
                grouper.close();
                writer.close();
                ctx.getCounterContext()
                        .getCounter(
                                "costmodel.operator." + LocalGroupOperatorDescriptor.class.getSimpleName() + "."
                                        + partition + ".optional.inputFrameCount", true)
                        .update(this.debugInputFrameCount);

                // elapsed time
                this.debugElapsedTime = System.currentTimeMillis() - debugElapsedTime;
                ctx.getCounterContext()
                        .getCounter(
                                "costmodel.operator." + LocalGroupOperatorDescriptor.class.getSimpleName() + "."
                                        + partition + ".optional.elapsedTime", true).update(this.debugElapsedTime);
            }

        };
    }

    public static final int MIN_RECURSIVE_STEP = 100;

    public static int computeMinRecursiveStepLevel(long recordsToProcess, int recordSizeInBytes, int memoryInPages,
            int pageSizeInBytes) {
        return (int) Math.max(MIN_RECURSIVE_STEP,
                computeMaxRecursiveLevel(recordsToProcess, recordSizeInBytes, memoryInPages, pageSizeInBytes));
    }

    public static int computeMaxRecursiveLevel(long recordsToProcess, int recordSizeInBytes, int memoryInPages,
            int pageSizeInBytes) {
        return (int) Math.max(
                1,
                Math.log(((double) recordsToProcess) * recordSizeInBytes)
                        / Math.log(((double) memoryInPages) * pageSizeInBytes));
    }
}
