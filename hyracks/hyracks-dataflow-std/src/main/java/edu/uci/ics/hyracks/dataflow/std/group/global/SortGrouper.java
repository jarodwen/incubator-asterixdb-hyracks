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
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.GrouperFlushOption;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IGrouperFlushOption.GroupOutputState;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalCommonCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalSortCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.RequiredCounters;

/**
 * An implementation of aggregating each frame of the input data using sort-based approach.
 */
public class SortGrouper extends AbstractHistogramPushBasedGrouper {

    private static final int POINTER_LENGTH = 3;
    private static final int INT_SIZE = 4;

    private final INormalizedKeyComputer nkc;
    private final IBinaryComparator[] comparators;

    protected List<ByteBuffer> buffers;
    protected FrameTupleAccessor bufferTupleAccessor;
    private FrameTupleAccessor bufferTupleAccessorForComparison;

    private ByteBuffer outFrame;
    private ArrayTupleBuilder tupleBuilder;
    private FrameTupleAppender appender;

    private int dataFrameCount;
    protected int[] tPointers;
    private int[] tPointersTemp;
    protected int tupleCount;

    private IAggregatorDescriptor aggregator;
    private IAggregatorDescriptor merger;
    private AggregateState aggregateState;

    private byte[] groupResultCache;
    private ByteBuffer groupResultCacheBuffer;
    private FrameTupleAppender groupResultCacheAppender;
    private FrameTupleAccessor groupResultCacheAccessor;

    private long debugOptionalSortCPUCompare = 0, debugOptionalSortCPUCopy = 0, debugOptionalIO = 0;
    private long profileCPU, profileIOInNetwork, profileIOInDisk, profileIOOutDisk, profileIOOutNetwork,
            profileInRecords, profileInFrames, profileOutRecords, profileOutFrames;

    public SortGrouper(
            IHyracksTaskContext ctx,
            int[] keyFields,
            int[] decorFields,
            int framesLimit,
            IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories,
            IFrameWriter outputWriter,
            boolean isGenerateRuns) throws HyracksDataException {
        super(ctx, keyFields, decorFields, framesLimit, aggregatorFactory, mergerFactory, inRecDesc, outRecDesc, false,
                outputWriter, isGenerateRuns);

        this.nkc = (firstKeyNormalizerFactory == null) ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
    }

    public void open() throws HyracksDataException {

        this.buffers = new ArrayList<ByteBuffer>();
        this.bufferTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
        this.bufferTupleAccessorForComparison = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecDesc, outRecDesc, keyFields, keyFields, null);
        int[] storedKeys = new int[keyFields.length];
        for (int i = 0; i < storedKeys.length; i++) {
            storedKeys[i] = i;
        }
        this.merger = mergerFactory.createAggregator(ctx, outRecDesc, outRecDesc, storedKeys, storedKeys, null);

        this.aggregateState = this.aggregator.createAggregateStates();

        this.outFrame = ctx.allocateFrame();
        this.tupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.dataFrameCount = 0;
        this.tupleCount = 0;

        resetHistogram();

        this.debugCounters.reset();
    }

    @Override
    public void reset() throws HyracksDataException {
        super.reset();

        this.dataFrameCount = 0;
        this.tupleCount = 0;
        resetHistogram();
        this.aggregateState.reset();
        this.tupleBuilder.reset();
        if (this.groupResultCacheBuffer != null) {
            groupResultCacheAppender.reset(groupResultCacheBuffer, true);
        }
        if (this.outFrame != null) {
            this.appender.reset(outFrame, true);
        }
    }

    public int getFrameCount() {
        return dataFrameCount;
    }

    public void nextFrame(
            ByteBuffer buffer) throws HyracksDataException {

        profileIOInNetwork++;
        profileInFrames++;
        profileInRecords += buffer.getInt(buffer.capacity() - INT_SIZE);

        ByteBuffer copyFrame;
        if (dataFrameCount == buffers.size()) {
            if (dataFrameCount < framesLimit) {
                buffers.add(ctx.allocateFrame());
            } else {
                if (isGenerateRuns) {
                    IFrameWriter dumpWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(SortGrouper.class
                            .getSimpleName()), ctx.getIOManager());
                    dumpWriter.open();
                    flush(dumpWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE);
                    RunFileReader runReader = ((RunFileWriter) dumpWriter).createReader();
                    this.runReaders.add(runReader);
                    dumpWriter.close();
                    this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                } else {
                    flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE);
                }
                // reset the data frame count
                for (ByteBuffer buf : buffers) {
                    buf.putInt(frameSize - INT_SIZE, 0);
                }
                this.dataFrameCount = 0;
                this.tupleCount = 0;
            }
        }
        copyFrame = buffers.get(dataFrameCount);
        FrameUtils.copy(buffer, copyFrame);
        ++dataFrameCount;
    }

    private void sortFrames() throws HyracksDataException {
        int nBuffers = dataFrameCount;
        tupleCount = 0;
        for (int i = 0; i < nBuffers; ++i) {
            bufferTupleAccessor.reset(buffers.get(i));
            tupleCount += bufferTupleAccessor.getTupleCount();
        }

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_INPUT, tupleCount);
        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_INPUT, dataFrameCount);

        tPointers = (tPointers == null || tPointers.length < tupleCount * POINTER_LENGTH) ? new int[tupleCount
                * POINTER_LENGTH] : tPointers;
        int ptr = 0;
        for (int i = 0; i < nBuffers; ++i) {
            bufferTupleAccessor.reset(buffers.get(i));
            int tCount = bufferTupleAccessor.getTupleCount();
            byte[] array = bufferTupleAccessor.getBuffer().array();
            for (int j = 0; j < tCount; ++j) {

                // do histogram update if needed
                insertIntoHistogram(bufferTupleAccessor, j, keyFields);

                tPointers[ptr * POINTER_LENGTH] = i;
                tPointers[ptr * POINTER_LENGTH + 1] = j;
                if (keyFields.length > 0 && nkc != null) {
                    int tStart = bufferTupleAccessor.getTupleStartOffset(j);
                    int f0StartRel = bufferTupleAccessor.getFieldStartOffset(j, keyFields[0]);
                    int f0EndRel = bufferTupleAccessor.getFieldEndOffset(j, keyFields[0]);
                    int f0Start = f0StartRel + tStart + bufferTupleAccessor.getFieldSlotsLength();
                    tPointers[ptr * POINTER_LENGTH + 2] = nkc.normalize(array, f0Start, f0EndRel - f0StartRel);
                } else {
                    tPointers[ptr * POINTER_LENGTH + 2] = 0;
                }
                ++ptr;
            }
        }
        if (tupleCount > 0) {
            tPointersTemp = new int[tPointers.length];
            sort(0, tupleCount);
        }
    }

    private void sort(
            int offset,
            int length) {
        int step = 1;
        int len = length;
        int end = offset + len;
        /** bottom-up merge */
        while (step < len) {
            /** merge */
            for (int i = offset; i < end; i += 2 * step) {
                int next = i + step;
                if (next < end) {
                    merge(i, next, step, Math.min(step, end - next));
                } else {
                    System.arraycopy(tPointers, i * POINTER_LENGTH, tPointersTemp, i * POINTER_LENGTH, (end - i)
                            * POINTER_LENGTH);
                }
            }
            /** prepare next phase merge */
            step *= 2;
            int[] tmp = tPointersTemp;
            tPointersTemp = tPointers;
            tPointers = tmp;
        }
    }

    /** Merge two subarrays into one */
    private void merge(
            int start1,
            int start2,
            int len1,
            int len2) {
        int targetPos = start1;
        int pos1 = start1;
        int pos2 = start2;
        int end1 = start1 + len1 - 1;
        int end2 = start2 + len2 - 1;
        while (pos1 <= end1 && pos2 <= end2) {
            int cmp = compare(pos1, pos2);
            if (cmp <= 0) {
                copy(pos1, targetPos);
                pos1++;
            } else {
                copy(pos2, targetPos);
                pos2++;
            }
            targetPos++;
        }
        if (pos1 <= end1) {
            int rest = end1 - pos1 + 1;
            System.arraycopy(tPointers, pos1 * POINTER_LENGTH, tPointersTemp, targetPos * POINTER_LENGTH, rest
                    * POINTER_LENGTH);
        }
        if (pos2 <= end2) {
            int rest = end2 - pos2 + 1;
            System.arraycopy(tPointers, pos2 * POINTER_LENGTH, tPointersTemp, targetPos * POINTER_LENGTH, rest
                    * POINTER_LENGTH);
        }
    }

    protected int compare(
            int tp1,
            int tp2) {

        debugOptionalSortCPUCompare++;
        profileCPU++;

        int buf1 = tPointers[tp1 * POINTER_LENGTH];
        int tid1 = tPointers[tp1 * POINTER_LENGTH + 1];
        int nk1 = tPointers[tp1 * POINTER_LENGTH + 2];

        int buf2 = tPointers[tp2 * POINTER_LENGTH];
        int tid2 = tPointers[tp2 * POINTER_LENGTH + 1];
        int nk2 = tPointers[tp2 * POINTER_LENGTH + 2];

        if (nk1 != nk2) {
            return ((((long) nk1) & 0xffffffffL) < (((long) nk2) & 0xffffffffL)) ? -1 : 1;
        }
        bufferTupleAccessor.reset(buffers.get(buf1));
        bufferTupleAccessorForComparison.reset(buffers.get(buf2));
        byte[] b1 = bufferTupleAccessor.getBuffer().array();
        byte[] b2 = bufferTupleAccessorForComparison.getBuffer().array();
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = keyFields[f];
            int s1 = bufferTupleAccessor.getTupleStartOffset(tid1) + bufferTupleAccessor.getFieldSlotsLength()
                    + +bufferTupleAccessor.getFieldStartOffset(tid1, fIdx);
            int l1 = bufferTupleAccessor.getFieldLength(tid1, fIdx);

            int s2 = bufferTupleAccessorForComparison.getTupleStartOffset(tid2)
                    + bufferTupleAccessor.getFieldSlotsLength()
                    + bufferTupleAccessorForComparison.getFieldStartOffset(tid2, fIdx);
            int l2 = bufferTupleAccessorForComparison.getFieldLength(tid2, fIdx);
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    protected boolean sameGroup(
            FrameTupleAccessor a1,
            int t1Idx,
            FrameTupleAccessor a2,
            int t2Idx) {
        debugOptionalSortCPUCompare++;
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

    private void copy(
            int src,
            int dest) {
        debugOptionalSortCPUCopy++;
        for (int i = 0; i < POINTER_LENGTH; i++) {
            tPointersTemp[dest * POINTER_LENGTH + i] = tPointers[src * POINTER_LENGTH + i];
        }
    }

    protected void flush(
            IFrameWriter writer,
            GrouperFlushOption flushOption) throws HyracksDataException {

        // sort the data before flushing
        sortFrames();

        if (tupleCount <= 0) {
            return;
        }

        for (int ptr = 0; ptr < tupleCount; ptr++) {
            int bufIdx = tPointers[ptr * POINTER_LENGTH];
            int tupleIdx = tPointers[ptr * POINTER_LENGTH + 1];
            bufferTupleAccessor.reset(buffers.get(bufIdx));
            if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {

                groupResultCacheAccessor.reset(ByteBuffer.wrap(groupResultCache));

                if (sameGroup(bufferTupleAccessor, tupleIdx, groupResultCacheAccessor, 0)) {
                    // find match: do aggregation
                    int groupCacheStartOffset = groupResultCacheAccessor.getTupleStartOffset(0);
                    aggregator.aggregate(bufferTupleAccessor, tupleIdx, groupResultCacheAccessor.getBuffer().array(),
                            groupCacheStartOffset, groupResultCacheAccessor.getTupleEndOffset(0)
                                    - groupCacheStartOffset, aggregateState);
                    continue;
                } else {
                    // write the cached group into the final output
                    writeOutput(groupResultCacheAccessor, 0, writer,
                            flushOption.getOutputState() == GroupOutputState.RESULT_STATE);
                }
            }

            tupleBuilder.reset();

            for (int i : keyFields) {
                tupleBuilder.addField(bufferTupleAccessor, tupleIdx, i);
            }
            for (int i : decorFields) {
                tupleBuilder.addField(bufferTupleAccessor, tupleIdx, i);
            }
            aggregator.init(tupleBuilder, bufferTupleAccessor, tupleIdx, aggregateState);

            // enlarge the cache buffer if needed
            int requiredSize = tupleBuilder.getSize() + tupleBuilder.getFieldEndOffsets().length * INT_SIZE + 2
                    * INT_SIZE;

            if (groupResultCache == null || groupResultCache.length < requiredSize) {
                groupResultCache = new byte[requiredSize];
                groupResultCacheAppender = new FrameTupleAppender(groupResultCache.length);
                groupResultCacheBuffer = ByteBuffer.wrap(groupResultCache);
                groupResultCacheAccessor = new FrameTupleAccessor(groupResultCache.length, outRecDesc);
            }

            groupResultCacheAppender.reset(groupResultCacheBuffer, true);
            if (!groupResultCacheAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("The partial result is too large to be initialized in a frame.");
            }

            groupResultCacheAccessor.reset(groupResultCacheBuffer);
        }

        if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {
            writeOutput(groupResultCacheAccessor, 0, writer,
                    flushOption.getOutputState() == GroupOutputState.RESULT_STATE);
            if (appender.getTupleCount() > 0) {

                profileOutFrames++;
                profileOutRecords += appender.getTupleCount();

                FrameUtils.flushFrame(outFrame, writer);

                if (isGenerateRuns) {
                    profileIOOutDisk++;
                } else {
                    profileIOOutNetwork++;
                }
                this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
                this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                        appender.getTupleCount());
                // Try to reset the output frame
                appender.reset(outFrame, true);
                debugOptionalIO++;
            }
        }
    }

    public void close() throws HyracksDataException {

        this.dumpAndCleanDebugCounters();

        this.buffers.clear();
        aggregateState.close();
        this.outFrame = null;
    }

    private void writeOutput(
            FrameTupleAccessor lastTupleAccessor,
            int lastTupleIndex,
            IFrameWriter writer,
            boolean useFinalMerger) throws HyracksDataException {

        if (appender == null) {
            appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(outFrame, true);
        }

        tupleBuilder.reset();
        for (int j = 0; j < keyFields.length + decorFields.length; j++) {
            tupleBuilder.addField(lastTupleAccessor, lastTupleIndex, j);
        }

        if (useFinalMerger) {
            merger.outputFinalResult(tupleBuilder, lastTupleAccessor, lastTupleIndex, aggregateState);
        } else {
            aggregator.outputFinalResult(tupleBuilder, lastTupleAccessor, lastTupleIndex, aggregateState);
        }

        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {

            profileOutFrames++;
            profileOutRecords += appender.getTupleCount();

            FrameUtils.flushFrame(outFrame, writer);
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                    appender.getTupleCount());

            if (isGenerateRuns) {
                profileIOOutDisk++;
            } else {
                profileIOOutNetwork++;
            }

            this.debugOptionalIO++;
            appender.reset(outFrame, true);
            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        groupResultCacheAppender.reset(groupResultCacheBuffer, true);

    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public List<RunFileReader> getOutputRunReaders() throws HyracksDataException {
        return runReaders;
    }

    @Override
    public int getRunsCount() {
        return this.runReaders.size();
    }

    protected void dumpAndCleanDebugCounters() {

        this.debugCounters.updateRequiredCounter(RequiredCounters.CPU, debugOptionalSortCPUCompare);
        this.debugCounters.updateRequiredCounter(RequiredCounters.IO_OUT_DISK, debugOptionalIO);

        this.debugCounters.updateRequiredCounter(RequiredCounters.IN_FRAMES, profileInFrames);
        this.debugCounters.updateRequiredCounter(RequiredCounters.IN_RECOEDS, profileInRecords);
        this.debugCounters.updateRequiredCounter(RequiredCounters.OUT_FRAMES, profileOutFrames);
        this.debugCounters.updateRequiredCounter(RequiredCounters.OUT_RECORDS, profileOutRecords);

        this.debugCounters.updateOptionalSortCounter(OptionalSortCounters.CPU_COMPARE, debugOptionalSortCPUCompare);
        this.debugCounters.updateOptionalSortCounter(OptionalSortCounters.CPU_COPY, debugOptionalSortCPUCopy);
        this.debugCounters.dumpCounters(ctx.getCounterContext());

        this.debugOptionalIO = 0;
        this.debugOptionalSortCPUCompare = 0;
        this.debugOptionalSortCPUCopy = 0;
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

        profileCPU = 0;
        profileIOInDisk = 0;
        profileIOInNetwork = 0;
        profileIOOutDisk = 0;
        profileIOOutNetwork = 0;

        this.profileInRecords = 0;
        this.profileInFrames = 0;
        this.profileOutRecords = 0;
        this.profileOutFrames = 0;
    }

    @Override
    public List<Long> getOutputRunSizeInRows() throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getRecordsCompletelyAggregated() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getGroupsCompletelyAggregated() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public List<Long> getOutputGroupsInRows() throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }
}
