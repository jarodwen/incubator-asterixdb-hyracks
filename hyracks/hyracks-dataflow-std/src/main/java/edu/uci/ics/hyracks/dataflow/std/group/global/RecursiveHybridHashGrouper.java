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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.DynamicHybridHashGrouper.PartSpillStrategy;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IFrameWriterRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalCommonCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashFunctionFamilyFactoryAdapter;

public class RecursiveHybridHashGrouper implements IFrameWriter {

    private IFrameWriterRunGenerator grouper;

    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    protected final int[] decorFields;
    private final int framesLimit, frameSize;
    private final int tableSize;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;

    private final long inputRecordCount, outputGroupCount;
    private final int groupStateSizeInBytes;
    private final double fudgeFactor;

    private final boolean useDynamicDestaging;

    /**
     * whether to further partition resident partitions
     */
    private final boolean enableResidentPart;

    private final boolean useBloomfilter;

    private final IFrameWriter outputWriter;

    private final int hashLevelSeed;
    private int hashLevelSeedVariable = 0;

    private int[] keyFieldsInGroupState, decorFieldsInGroupState;

    private int gracePartitions, hybridHashSpilledPartitions, hybridHashResidentPartitions;

    private int maxRecursionLevel;

    /**
     * The minimum number of frames per resident partition. This is used to
     * compute the number of resident partitions.
     */
    private final int minFramesPerResPart;

    /**
     * Whether the last resident partition should be pinned in the dynamic-destaging algorithm.
     */
    private final boolean pinLastResidentPart;

    /**
     * The strategy to pick the next partition to spill
     */
    private PartSpillStrategy partSpillStrategy;

    private final OperatorDebugCounterCollection debugCounters;

    private long profileCPU, profileIOInNetwork, profileIOInDisk, profileIOOutDisk, profileIOOutNetwork;

    public RecursiveHybridHashGrouper(
            IHyracksTaskContext ctx,
            int[] keyFields,
            int[] decorFields,
            int framesLimit,
            int tableSize,
            long inputRecordCount,
            long outputGroupCount,
            int groupStateSizeInBytes,
            double fudgeFactor,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies,
            IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory,
            RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor,
            int hashLevelSeed,
            IFrameWriter outputWriter,
            boolean useDynamic,
            boolean enableResidentPart,
            boolean useBloomfilter,
            int minFramesPerResPart,
            boolean pinLastResPart,
            PartSpillStrategy partSpillStrategy) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.frameSize = ctx.getFrameSize();
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.hashFunctionFamilies = hashFunctionFamilies;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;
        this.hashLevelSeed = hashLevelSeed;
        this.outputWriter = outputWriter;
        this.minFramesPerResPart = minFramesPerResPart;

        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;

        this.inputRecordCount = inputRecordCount;
        this.outputGroupCount = outputGroupCount;
        this.groupStateSizeInBytes = groupStateSizeInBytes;
        this.fudgeFactor = fudgeFactor;

        this.useDynamicDestaging = useDynamic;
        this.enableResidentPart = enableResidentPart;
        this.useBloomfilter = useBloomfilter;
        this.pinLastResidentPart = pinLastResPart;

        this.partSpillStrategy = partSpillStrategy;

        this.debugCounters = new OperatorDebugCounterCollection("costmodel.operator." + this.getClass().getSimpleName()
                + "." + String.valueOf(Thread.currentThread().getId()));
    }

    @Override
    public void open() throws HyracksDataException {

        IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[this.hashFunctionFamilies.length];
        for (int i = 0; i < hashFunctionFactories.length; i++) {
            hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                    this.hashFunctionFamilies[i], hashLevelSeed + hashLevelSeedVariable);
        }
        hashLevelSeedVariable++;

        gracePartitions = computeGracePartitions(framesLimit, frameSize, outputGroupCount, groupStateSizeInBytes,
                fudgeFactor, (useDynamicDestaging ? 2 : 1));
        hybridHashSpilledPartitions = computeHybridHashSpilledPartitions(framesLimit, frameSize, outputGroupCount,
                groupStateSizeInBytes, gracePartitions, fudgeFactor, (useDynamicDestaging ? 2 : 1));
        hybridHashResidentPartitions = computeHybridHashResidentPartitions(framesLimit, hybridHashSpilledPartitions,
                minFramesPerResPart);
        maxRecursionLevel = getMaxLevelsIfUsingSortGrouper(framesLimit, inputRecordCount, groupStateSizeInBytes);

        this.debugCounters.updateOptionalCustomizedCounter(".partition.hybrid.0.keys", outputGroupCount
                / gracePartitions);
        this.debugCounters.updateOptionalCustomizedCounter(".partition.hybrid.0.records", inputRecordCount
                / gracePartitions);
        this.debugCounters.updateOptionalCustomizedCounter(".partition.grace", gracePartitions);
        this.debugCounters.updateOptionalCustomizedCounter(".partition.hybrid.0.partitions",
                hybridHashSpilledPartitions);
        this.debugCounters.updateOptionalCustomizedCounter(".partition.maxRecursionLevel", maxRecursionLevel);

        if (gracePartitions > 1) {
            grouper = new GracePartitioner(ctx, framesLimit, gracePartitions, keyFields, hashLevelSeed
                    + hashLevelSeedVariable, hashFunctionFamilies, inRecordDesc);
            hashLevelSeedVariable += LocalGroupOperatorDescriptor.computeMaxRecursiveLevel(inputRecordCount,
                    groupStateSizeInBytes, framesLimit, frameSize);
        } else {
            if (useDynamicDestaging) {
                grouper = new DynamicHybridHashGrouper(ctx, keyFields, decorFields, framesLimit,
                        this.firstKeyNormalizerFactory, comparatorFactories, hashFunctionFactories, aggregatorFactory,
                        finalMergerFactory, inRecordDesc, outRecordDesc, false, outputWriter, true, tableSize,
                        hybridHashSpilledPartitions, useBloomfilter, pinLastResidentPart, partSpillStrategy);
            } else {
                grouper = new HybridHashGrouper(ctx, keyFields, decorFields, framesLimit,
                        this.firstKeyNormalizerFactory, comparatorFactories, hashFunctionFactories, aggregatorFactory,
                        finalMergerFactory, inRecordDesc, outRecordDesc, false, outputWriter, true, tableSize,
                        hybridHashSpilledPartitions, hybridHashResidentPartitions, useBloomfilter, enableResidentPart,
                        partSpillStrategy);
            }
        }
        grouper.open();
    }

    /**
     * Compute the number of partitions to be produced by the grace partitioner.
     * The grace partitioner partitions the
     * input data so that each partition contains no more than M^2 group states.
     * 
     * @param framesLimit
     * @param outputGroupCount
     * @param groupStateSizeInBytes
     * @param fudgeFactor
     * @return
     * @throws HyracksDataException
     */
    public static int computeGracePartitions(
            int framesLimit,
            int frameSize,
            long outputGroupCount,
            int groupStateSizeInBytes,
            double fudgeFactor,
            int minPartitions) throws HyracksDataException {
        int minGracePartitions = 1;

        while (computeHybridHashSpilledPartitions(framesLimit, frameSize, outputGroupCount, groupStateSizeInBytes,
                minGracePartitions, fudgeFactor, minPartitions) * fudgeFactor > framesLimit - 1) {
            minGracePartitions *= framesLimit;
        }

        return (int) Math.pow(framesLimit, (int) Math.ceil(Math.log(minGracePartitions) / Math.log(framesLimit)));
    }

    public static int computeHybridHashSpilledPartitions(
            int framesLimit,
            int frameSize,
            long outputGroupCount,
            int groupStateSizeInBytes,
            int gracePartitions,
            double fudgeFactor,
            int minPartitions) throws HyracksDataException {
        double partitionGroupSizeInFrames = (double) outputGroupCount / gracePartitions * groupStateSizeInBytes
                / frameSize;
        int parts = (int) Math.max(minPartitions,
                Math.ceil((partitionGroupSizeInFrames * fudgeFactor - framesLimit) / (framesLimit - 2)));

        return parts;
    }

    public static int computeHybridHashResidentPartitions(
            int framesLimit,
            int hybridHashSpilledPartitions,
            int minFramesPerPart) {
        return Math.max(1, (framesLimit - 1 - hybridHashSpilledPartitions) / minFramesPerPart);
    }

    /**
     * Compute the max level of recursion, based on the assumption that the
     * levels of hybrid hash should
     * not be more than the levels of merging used in a sort-based approach.
     * 
     * @param framesLimit
     * @param inputRecordCount
     * @param groupStateSizeInBytes
     * @return
     */
    protected int getMaxLevelsIfUsingSortGrouper(
            int framesLimit,
            long inputRecordCount,
            int groupStateSizeInBytes) {
        return (int) Math.ceil(Math.log((double) inputRecordCount * groupStateSizeInBytes * frameSize / framesLimit)
                / Math.log(framesLimit));
    }

    @Override
    public void nextFrame(
            ByteBuffer buffer) throws HyracksDataException {

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_INPUT, 1);
        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_INPUT,
                buffer.getInt(buffer.capacity() - 4));

        if (grouper instanceof GracePartitioner) {
            profileIOInNetwork++;
        }

        grouper.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        grouper.wrapup();
        List<RunFileReader> runs = grouper.getOutputRunReaders();
        // Get the number of records in the runs
        List<Long> groupsInRuns = grouper.getOutputRunSizeInRows();

        grouper.close();

        if (runs.size() <= 0) {
            return;
        }

        // try to estimate the collapsing ratio, assuming that the number of raw records in the given
        // run partition is known
        long recordsLeft = 0;
        if (groupsInRuns != null) {
            for (long gr : groupsInRuns) {
                recordsLeft += gr;
            }
        }
        double recordsAbsorbedExpected = framesLimit / fudgeFactor * frameSize / groupStateSizeInBytes;
        double recGroupRatio = (this.inputRecordCount - recordsLeft) / recordsAbsorbedExpected;
        if (recGroupRatio < 1) {
            recGroupRatio = 1;
        }

        List<Integer> runLevels = new LinkedList<Integer>();
        List<Integer> runPartitions = new LinkedList<Integer>();
        for (int i = 0; i < runs.size(); i++) {
            runLevels.add(0);
            if (grouper instanceof GracePartitioner) {
                runPartitions.add(this.hybridHashSpilledPartitions);
            } else {

                int newPartitions = computeHybridHashSpilledPartitions(framesLimit, frameSize,
                        (long) (groupsInRuns.get(i) / recGroupRatio), groupStateSizeInBytes, 1, fudgeFactor,
                        (useDynamicDestaging ? 2 : 1));
                runPartitions.add(newPartitions);
            }
        }

        recursiveRunProcess(runs, runLevels, runPartitions, (grouper instanceof GracePartitioner));

        ctx.getCounterContext().getCounter("profile.cpu." + this.debugCounters.getDebugID(), true).update(profileCPU);
        ctx.getCounterContext().getCounter("profile.io.in.disk." + this.debugCounters.getDebugID(), true)
                .update(profileIOInDisk);
        ctx.getCounterContext().getCounter("profile.io.in.network." + this.debugCounters.getDebugID(), true)
                .update(profileIOInNetwork);
        ctx.getCounterContext().getCounter("profile.io.out.disk." + this.debugCounters.getDebugID(), true)
                .update(profileIOOutDisk);
        ctx.getCounterContext().getCounter("profile.io.out.network." + this.debugCounters.getDebugID(), true)
                .update(profileIOOutNetwork);

        profileCPU = 0;
        profileIOInDisk = 0;
        profileIOInNetwork = 0;
        profileIOOutDisk = 0;
        profileIOOutNetwork = 0;

        this.debugCounters.dumpCounters(ctx.getCounterContext());
        this.debugCounters.reset();
    }

    private void recursiveRunProcess(
            List<RunFileReader> runs,
            List<Integer> runLevels,
            List<Integer> runPartitions,
            boolean isRawData) throws HyracksDataException {
        if (keyFieldsInGroupState == null) {
            keyFieldsInGroupState = new int[keyFields.length];
            for (int i = 0; i < keyFields.length; i++) {
                keyFieldsInGroupState[i] = i;
            }
        }

        if (decorFieldsInGroupState == null) {
            decorFieldsInGroupState = new int[decorFields.length];
            for (int i = 0; i < decorFields.length; i++) {
                decorFieldsInGroupState[i] = i + keyFields.length;
            }
        }

        int hashLevel = runLevels.get(0);

        IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[this.hashFunctionFamilies.length];
        for (int i = 0; i < hashFunctionFactories.length; i++) {
            hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                    this.hashFunctionFamilies[i], hashLevelSeed + hashLevelSeedVariable + hashLevel);
        }

        ByteBuffer inputBuffer = ctx.allocateFrame();

        int runIdx = 0;
        int originalRunsCount = runs.size();

        while (runs.size() > 0) {
            RunFileReader runReader = runs.remove(0);
            int runLevel = runLevels.remove(0);
            int runPartition = runPartitions.remove(0);

            if (runLevel != hashLevel) {
                // reset run index and hash function for run file
                runIdx = 0;
                hashLevel = runLevel;
                for (int i = 0; i < hashFunctionFactories.length; i++) {
                    hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                            this.hashFunctionFamilies[i], hashLevelSeed + hashLevelSeedVariable + hashLevel);
                }
            }

            this.debugCounters.updateOptionalCustomizedCounter(".partition.hybrid." + runLevel + ".runs", 1);
            this.debugCounters.updateOptionalCustomizedCounter(".partition.hybrid." + runLevel + "." + runIdx
                    + ".partitions", runPartition);

            int newTableSize = LocalGroupOperatorDescriptor.computeHashtableSlots(framesLimit - 1, frameSize,
                    groupStateSizeInBytes, LocalGroupOperatorDescriptor.HT_SLOT_CAP_RATIO,
                    LocalGroupOperatorDescriptor.HT_FRAME_REF_SIZE, LocalGroupOperatorDescriptor.HT_TUPLE_REF_SIZE,
                    this.useBloomfilter, LocalGroupOperatorDescriptor.HT_MINI_BLOOM_FILTER_SIZE, framesLimit,
                    runPartition);

            IFrameWriterRunGenerator hybridHashGrouper;
            if (isRawData && originalRunsCount > 0) {
                if (useDynamicDestaging) {
                    hybridHashGrouper = new DynamicHybridHashGrouper(ctx, keyFields, decorFieldsInGroupState,
                            framesLimit, firstKeyNormalizerFactory, comparatorFactories, hashFunctionFactories,
                            aggregatorFactory, partialMergerFactory, inRecordDesc, outRecordDesc, true, outputWriter,
                            true, newTableSize, runPartition, this.useBloomfilter, pinLastResidentPart,
                            partSpillStrategy);
                } else {
                    hybridHashGrouper = new HybridHashGrouper(ctx, keyFields, decorFieldsInGroupState, framesLimit,
                            firstKeyNormalizerFactory, comparatorFactories, hashFunctionFactories, aggregatorFactory,
                            partialMergerFactory, inRecordDesc, outRecordDesc, true, outputWriter, true, newTableSize,
                            runPartition, computeHybridHashResidentPartitions(framesLimit, runPartition,
                                    minFramesPerResPart), this.useBloomfilter, enableResidentPart, partSpillStrategy);
                }
                originalRunsCount--;
            } else {
                if (useDynamicDestaging) {
                    hybridHashGrouper = new DynamicHybridHashGrouper(ctx, keyFieldsInGroupState,
                            decorFieldsInGroupState, framesLimit, firstKeyNormalizerFactory, comparatorFactories,
                            hashFunctionFactories, partialMergerFactory, finalMergerFactory, outRecordDesc,
                            outRecordDesc, true, outputWriter, true, newTableSize, runPartition, this.useBloomfilter,
                            pinLastResidentPart, partSpillStrategy);
                } else {
                    hybridHashGrouper = new HybridHashGrouper(ctx, keyFieldsInGroupState, decorFieldsInGroupState,
                            framesLimit, firstKeyNormalizerFactory, comparatorFactories, hashFunctionFactories,
                            partialMergerFactory, finalMergerFactory, outRecordDesc, outRecordDesc, true, outputWriter,
                            true, newTableSize, runPartition, computeHybridHashResidentPartitions(framesLimit,
                                    runPartition, minFramesPerResPart), useBloomfilter, enableResidentPart,
                            partSpillStrategy);
                }
            }

            long framesProcessed = 0;
            long recordsProcessed = 0;

            hybridHashGrouper.open();
            runReader.open();

            while (runReader.nextFrame(inputBuffer)) {
                framesProcessed++;
                recordsProcessed += inputBuffer.getInt(frameSize - 4);
                profileIOInDisk++;
                // do adjust on the network cost
                profileIOInNetwork--;
                hybridHashGrouper.nextFrame(inputBuffer);
            }

            runReader.close();

            this.debugCounters.updateOptionalCustomizedCounter(".partition.hybrid." + runLevel + "." + runIdx
                    + ".frame.input", framesProcessed);
            this.debugCounters.updateOptionalCustomizedCounter(".partition.hybrid." + runLevel + "." + runIdx
                    + ".record.input", recordsProcessed);

            hybridHashGrouper.wrapup();

            long rawRecordsInResidentPartition = hybridHashGrouper.getRecordsCompletelyAggregated();
            long groupsInResidentPartition = hybridHashGrouper.getGroupsCompletelyAggregated();
            List<Long> rawRecordsInSpillingPartitions = hybridHashGrouper.getOutputRunSizeInRows();
            List<RunFileReader> runsFromHybridHash = hybridHashGrouper.getOutputRunReaders();

            // try to estimate the collapsing ratio, assuming that the number of raw records in the given
            // run partition is known
            long recordsLeft = 0;
            if (rawRecordsInSpillingPartitions != null) {
                for (long gr : rawRecordsInSpillingPartitions) {
                    recordsLeft += gr;
                }
            }
            double recordsAbsorbedExpected = framesLimit / fudgeFactor * frameSize / groupStateSizeInBytes;
            double recGroupRatio = (this.inputRecordCount - recordsLeft) / recordsAbsorbedExpected;
            if (recGroupRatio < 1) {
                recGroupRatio = 1;
            }

            hybridHashGrouper.close();

            while (runsFromHybridHash.size() > 0) {
                RunFileReader runReaderFromHybridHash = runsFromHybridHash.remove(0);

                if (runLevel + 1 > maxRecursionLevel) {
                    // fallback to hash-sort algorithm
                    HashGroupSortMergeGrouper hashSortGrouper = new HashGroupSortMergeGrouper(ctx,
                            keyFieldsInGroupState, decorFieldsInGroupState, framesLimit, tableSize,
                            firstKeyNormalizerFactory, comparatorFactories, hashFunctionFactories, aggregatorFactory,
                            partialMergerFactory, finalMergerFactory, outRecordDesc, outRecordDesc, outputWriter);
                    hashSortGrouper.open();
                    runReaderFromHybridHash.open();

                    this.debugCounters.updateOptionalCustomizedCounter(".fallback.runs", 1);

                    framesProcessed = 0;

                    while (runReaderFromHybridHash.nextFrame(inputBuffer)) {
                        framesProcessed++;
                        profileIOInDisk++;
                        // do adjust on the network cost
                        profileIOInNetwork--;
                        hashSortGrouper.nextFrame(inputBuffer);
                    }

                    this.debugCounters.updateOptionalCustomizedCounter(".fallback.frame.input", framesProcessed);

                    hashSortGrouper.close();
                    continue;
                }

                long rawRecordsInRun = rawRecordsInSpillingPartitions.remove(0);
                int recursivePartition = computeHybridHashSpilledPartitions(framesLimit, frameSize,
                        (int) ((double) rawRecordsInRun / recGroupRatio), groupStateSizeInBytes, 1, fudgeFactor,
                        (useDynamicDestaging ? 2 : 1));
                runs.add(runReaderFromHybridHash);
                runLevels.add(runLevel + 1);
                runPartitions.add(recursivePartition);
            }

            runIdx++;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }
}
