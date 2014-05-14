package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.RunMergingGroupingFrameReader;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalCommonCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.RequiredCounters;

public class MergeGrouper {

    private final IHyracksTaskContext ctx;

    private final int[] keyFields, decorFields;
    private final RecordDescriptor inRecDesc, outRecDesc;

    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer nmkComputer;

    private final int framesLimit;

    private final IAggregatorDescriptor partialMerger, finalMerger;
    private AggregateState mergeState;

    List<RunFileReader> runs;

    List<ByteBuffer> inFrames;
    ByteBuffer outFrame, writerFrame;
    FrameTupleAppender outFrameAppender, writerAppender;
    ArrayTupleBuilder flushTupleBuilder;
    FrameTupleAccessor outFrameAccessor;
    int[] currentFrameIndexInRun, currentRunFrames, currentBucketInRun;
    int runFrameLimit = 1;

    private final ITuplePartitionComputerFactory tuplePartitionComputerFactory;
    private final int partitions;

    // For debugging

    private final OperatorDebugCounterCollection debugCounters;

    private boolean debugDumpToFileFlag = false;
    private long profileCPU, profileIOInNetwork, profileIOInDisk, profileIOOutDisk, profileIOOutNetwork;

    public MergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory normalizerFactory,
            IAggregatorDescriptorFactory partialMergerFactory, IAggregatorDescriptorFactory finalMergerFactory,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc) throws HyracksDataException {
        this(ctx, keyFields, decorFields, framesLimit, 1, comparatorFactories, normalizerFactory, null,
                partialMergerFactory, finalMergerFactory, inRecDesc, outRecDesc);
    }

    public MergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit, int partitions,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory normalizerFactory,
            IBinaryHashFunctionFactory[] hashFunctionFactories, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc)
            throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < this.comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.nmkComputer = (normalizerFactory == null) ? null : normalizerFactory.createNormalizedKeyComputer();

        this.partialMerger = partialMergerFactory.createAggregator(ctx, inRecDesc, outRecDesc, keyFields, keyFields,
                null);
        this.finalMerger = finalMergerFactory.createAggregator(ctx, outRecDesc, outRecDesc, keyFields, keyFields, null);
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;

        this.tuplePartitionComputerFactory = (hashFunctionFactories == null) ? null
                : new FieldHashPartitionComputerFactory(keyFields, hashFunctionFactories);
        this.partitions = partitions;

        this.mergeState = partialMerger.createAggregateStates();

        this.debugCounters = new OperatorDebugCounterCollection("costmodel.operator." + this.getClass().getSimpleName()
                + "." + String.valueOf(Thread.currentThread().getId()));
    }

    public void process(List<RunFileReader> runFiles, IFrameWriter writer) throws HyracksDataException {

        runs = runFiles;

        writer.open();
        try {
            if (runs.size() > 0) {
                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.allocateFrame();
                outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
                outFrameAppender.reset(outFrame, true);
                for (int i = 0; i < framesLimit - 1; ++i) {
                    inFrames.add(ctx.allocateFrame());
                }
                int maxMergeWidth = framesLimit - 1;
                while (runs.size() > maxMergeWidth) {
                    int generationSeparator = 0;
                    while (generationSeparator < runs.size() && runs.size() > maxMergeWidth) {
                        debugDumpToFileFlag = true;
                        int mergeWidth = Math.min(Math.min(runs.size() - generationSeparator, maxMergeWidth),
                                runs.size() - maxMergeWidth + 1);
                        FileReference newRun = ctx.createManagedWorkspaceFile(MergeGrouper.class.getSimpleName());
                        IFrameWriter mergeResultWriter = new RunFileWriter(newRun, ctx.getIOManager());
                        mergeResultWriter.open();
                        IFrameReader[] runCursors = new RunFileReader[mergeWidth];
                        for (int i = 0; i < mergeWidth; i++) {
                            runCursors[i] = runs.get(generationSeparator + i);
                        }
                        merge(mergeResultWriter, runCursors, false);
                        runs.subList(generationSeparator, mergeWidth + generationSeparator).clear();
                        runs.add(generationSeparator++, ((RunFileWriter) mergeResultWriter).createReader());
                        mergeResultWriter.close();
                        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                    }
                }
                if (!runs.isEmpty()) {
                    IFrameReader[] runCursors = new RunFileReader[runs.size()];
                    for (int i = 0; i < runCursors.length; i++) {
                        runCursors[i] = runs.get(i);
                    }
                    debugDumpToFileFlag = false;
                    merge(writer, runCursors, true);
                }
            }
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {

            this.debugCounters.dumpCounters(ctx.getCounterContext());
            this.debugCounters.reset();

            ctx.getCounterContext().getCounter("profile.cpu." + this.debugCounters.getDebugID(), true)
                    .update(profileCPU);
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

            writer.close();
        }
    }

    protected void merge(IFrameWriter mergeResultWriter, IFrameReader[] runCursors, boolean isFinal)
            throws HyracksDataException {

        IAggregatorDescriptor merger = (isFinal) ? finalMerger : partialMerger;

        RunMergingGroupingFrameReader mergeFrameReader = new RunMergingGroupingFrameReader(ctx, runCursors, inFrames,
                keyFields, decorFields, comparators, nmkComputer, (tuplePartitionComputerFactory == null) ? null
                        : tuplePartitionComputerFactory.createPartitioner(), partitions, merger, mergeState, inRecDesc,
                outRecDesc);
        mergeFrameReader.open();
        try {
            while (mergeFrameReader.nextFrame(outFrame)) {
                flushOutFrame(mergeResultWriter, isFinal);
            }
        } finally {
            if (writerAppender.getTupleCount() > 0) {
                if (debugDumpToFileFlag) {
                    this.debugCounters.updateRequiredCounter(RequiredCounters.IO_OUT_DISK, 2);
                } else {
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                            writerAppender.getTupleCount());
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
                }
                FrameUtils.flushFrame(writerFrame, mergeResultWriter);
                if (isFinal) {
                    profileIOOutNetwork++;
                } else {
                    profileIOOutDisk++;
                }
                writerAppender.reset(writerFrame, true);
            }
            mergeFrameReader.close();
        }
    }

    private void flushOutFrame(IFrameWriter writer, boolean isFinal) throws HyracksDataException {

        IAggregatorDescriptor merger = (isFinal) ? finalMerger : partialMerger;

        if (flushTupleBuilder == null) {
            flushTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFields().length);
        }

        if (writerFrame == null) {
            writerFrame = ctx.allocateFrame();
        }

        if (writerAppender == null) {
            writerAppender = new FrameTupleAppender(ctx.getFrameSize());
        }
        writerAppender.reset(writerFrame, false);

        if (outFrameAccessor == null) {
            outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), outRecDesc);
        }

        outFrameAccessor.reset(outFrame);

        if (debugDumpToFileFlag) {
            this.debugCounters.updateRequiredCounter(RequiredCounters.IO_OUT_DISK, 1);
        }

        for (int i = 0; i < outFrameAccessor.getTupleCount(); i++) {

            flushTupleBuilder.reset();

            for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                flushTupleBuilder.addField(outFrameAccessor, i, k);
            }

            merger.outputFinalResult(flushTupleBuilder, outFrameAccessor, i, mergeState);

            if (!writerAppender.append(flushTupleBuilder.getFieldEndOffsets(), flushTupleBuilder.getByteArray(), 0,
                    flushTupleBuilder.getSize())) {
                FrameUtils.flushFrame(writerFrame, writer);

                if (isFinal) {
                    profileIOOutNetwork++;
                } else {
                    profileIOOutDisk++;
                }

                if (debugDumpToFileFlag) {
                    this.debugCounters.updateRequiredCounter(RequiredCounters.IO_OUT_DISK, 2);
                } else {
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                            writerAppender.getTupleCount());
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
                }

                writerAppender.reset(writerFrame, true);
                if (!writerAppender.append(flushTupleBuilder.getFieldEndOffsets(), flushTupleBuilder.getByteArray(), 0,
                        flushTupleBuilder.getSize())) {
                    throw new HyracksDataException("Aggregation output is too large to be fit into a frame.");
                }
            }
        }
    }

}
