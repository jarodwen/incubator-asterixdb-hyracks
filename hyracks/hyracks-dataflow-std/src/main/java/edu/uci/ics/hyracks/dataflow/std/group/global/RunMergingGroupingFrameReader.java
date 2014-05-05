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
import java.util.Comparator;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.RunMergingFrameReader;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;

/**
 * An extended {@link RunMergingFrameReader} to support group-by during the merging.
 */
public class RunMergingGroupingFrameReader extends RunMergingFrameReader {

    private final IAggregatorDescriptor merger;
    private final AggregateState mergeState;

    private final int[] decorFields;

    private IFrameTupleAccessor outFrameAccessor;
    private ArrayTupleBuilder tupleBuilder;
    private boolean hasResultInTupleBuilder;

    /**
     * Used for the combined key (hash, keys).
     */
    private final ITuplePartitionComputer partitionComputer;
    private final int partitions;

    // For debugging
    private long compCounter = 0, inRecCounter = 0, outRecCounter = 0, outFrameCounter = 0, recCopyCounter = 0;
    private static String debugID = RunMergingGroupingFrameReader.class.getSimpleName();

    public RunMergingGroupingFrameReader(IHyracksTaskContext ctx, IFrameReader[] runCursors, List<ByteBuffer> inFrames,
            int[] keyFields, int[] decorFields, IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer,
            ITuplePartitionComputer partitionComputer, int partitions, IAggregatorDescriptor merger,
            AggregateState mergeState, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc) {
        // FIXME [jarodwen] use correct normalized key to replace null
        super(ctx, runCursors, inFrames, keyFields, comparators, nmkComputer, inRecordDesc);
        this.partitionComputer = partitionComputer;
        this.partitions = partitions;
        this.decorFields = decorFields;
        this.merger = merger;
        this.mergeState = mergeState;
        this.outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDesc);
        this.tupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFieldCount());
        this.hasResultInTupleBuilder = false;
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        outFrameAppender.reset(buffer, true);
        outFrameAccessor.reset(buffer);
        if (hasResultInTupleBuilder) {
            if (!outFrameAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("The tuple size is too large for a single frame.");
            }
            hasResultInTupleBuilder = false;
        }
        while (!topTuples.areRunsExhausted()) {

            ReferenceEntry top = topTuples.peek();
            int runIndex = top.getRunid();
            FrameTupleAccessor fta = top.getAccessor();
            int tupleIndex = top.getTupleIndex();

            int currentTupleInOutFrame = outFrameAccessor.getTupleCount() - 1;
            if (currentTupleInOutFrame < 0
                    || compareFrameTuples(fta, tupleIndex, outFrameAccessor, currentTupleInOutFrame) != 0) {

                tupleBuilder.reset();

                for (int k = 0; k < sortFields.length; k++) {
                    tupleBuilder.addField(fta, tupleIndex, sortFields[k]);
                }

                for (int k = 0; k < decorFields.length; k++) {
                    tupleBuilder.addField(fta, tupleIndex, decorFields[k]);
                }

                merger.init(tupleBuilder, fta, tupleIndex, mergeState);

                if (!outFrameAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                        tupleBuilder.getSize())) {
                    hasResultInTupleBuilder = true;
                    return true;
                }

                outRecCounter++;

            } else {
                /**
                 * if new tuple is in the same group of the
                 * current aggregator do merge and output to the
                 * outFrame
                 */

                int outTupleStartOffset = outFrameAccessor.getTupleStartOffset(currentTupleInOutFrame);
                merger.aggregate(fta, tupleIndex, outFrameAccessor.getBuffer().array(), outTupleStartOffset,
                        outFrameAccessor.getTupleEndOffset(currentTupleInOutFrame) - outTupleStartOffset, mergeState);

            }

            inRecCounter++;
            ++tupleIndexes[runIndex];
            setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
        }

        if (outFrameAppender.getTupleCount() > 0) {
            return true;
        }
        return false;
    }

    private int compareFrameTuples(IFrameTupleAccessor fta1, int j1, IFrameTupleAccessor fta2, int j2) {
        compCounter++;

        byte[] b1 = fta1.getBuffer().array();
        byte[] b2 = fta2.getBuffer().array();
        for (int f = 0; f < sortFields.length; ++f) {
            int fIdx = sortFields[f];
            int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength() + fta1.getFieldStartOffset(j1, fIdx);
            int l1 = fta1.getFieldLength(j1, fIdx);
            int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength() + fta2.getFieldStartOffset(j2, fIdx);
            int l2 = fta2.getFieldLength(j2, fIdx);
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @Override
    public void close() throws HyracksDataException {
        ctx.getCounterContext().getCounter(debugID + ".comparisons", true).update(compCounter);
        ctx.getCounterContext().getCounter(debugID + ".inputRecords", true).update(inRecCounter);
        ctx.getCounterContext().getCounter(debugID + ".outputRecords", true).update(outRecCounter);
        ctx.getCounterContext().getCounter(debugID + ".outputFrames", true).update(outFrameCounter);
        ctx.getCounterContext().getCounter(debugID + ".recordCopies", true).update(recCopyCounter);
        this.compCounter = 0;
        this.inRecCounter = 0;
        this.outRecCounter = 0;
        this.outFrameCounter = 0;
        this.recCopyCounter = 0;

        super.close();
    }

    @Override
    protected Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {

        return new Comparator<ReferenceEntry>() {

            @Override
            public int compare(ReferenceEntry tp1, ReferenceEntry tp2) {

                FrameTupleAccessor fta1 = (FrameTupleAccessor) tp1.getAccessor();
                FrameTupleAccessor fta2 = (FrameTupleAccessor) tp2.getAccessor();
                int j1 = tp1.getTupleIndex();
                int j2 = tp2.getTupleIndex();

                if (partitionComputer != null) {
                    try {

                        int h1 = partitionComputer.partition(fta1, j1, partitions);
                        int h2 = partitionComputer.partition(fta2, j2, partitions);
                        if (h1 != h2) {
                            return (h1 > h2) ? 1 : -1;
                        }
                    } catch (HyracksDataException hex) {
                        throw new IllegalStateException(hex);
                    }
                }

                byte[] b1 = fta1.getBuffer().array();
                byte[] b2 = fta2.getBuffer().array();
                for (int f = 0; f < sortFields.length; ++f) {
                    int fIdx = sortFields[f];
                    int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength()
                            + fta1.getFieldStartOffset(j1, fIdx);
                    int l1 = fta1.getFieldEndOffset(j1, fIdx) - fta1.getFieldStartOffset(j1, fIdx);
                    int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength()
                            + fta2.getFieldStartOffset(j2, fIdx);
                    int l2 = fta2.getFieldEndOffset(j2, fIdx) - fta2.getFieldStartOffset(j2, fIdx);
                    int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }

        };
    }
}
