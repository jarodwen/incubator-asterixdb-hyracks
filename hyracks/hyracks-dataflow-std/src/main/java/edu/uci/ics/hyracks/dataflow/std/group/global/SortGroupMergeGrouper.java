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
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class SortGroupMergeGrouper implements IFrameWriter {

    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    protected final int[] decorFields;
    private final int framesLimit;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;
    private final IFrameWriter outputWriter;

    private SortGrouper sortGrouper;

    public SortGroupMergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;
        this.outputWriter = outputWriter;

        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#init()
     */
    @Override
    public void open() throws HyracksDataException {
        sortGrouper = new SortGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory, finalMergerFactory,
                inRecordDesc, outRecordDesc, firstKeyNormalizerFactory, comparatorFactories, outputWriter, true);
        sortGrouper.open();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        sortGrouper.nextFrame(buffer);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#close()
     */
    @Override
    public void close() throws HyracksDataException {
        sortGrouper.wrapup();
        if (sortGrouper.getRunsCount() == 0) {
            // no run file has been generated in the sort grouper, so the content can be directly outputted
            sortGrouper.close();
        } else {
            List<RunFileReader> runs = sortGrouper.getOutputRunReaders();
            sortGrouper.close();
            int[] mergeKeyFields = new int[keyFields.length];
            for (int i = 0; i < keyFields.length; i++) {
                mergeKeyFields[i] = i;
            }
            int[] mergeDecorFields = new int[decorFields.length];
            for (int i = 0; i < decorFields.length; i++) {
                mergeDecorFields[i] = keyFields.length + i;
            }
            MergeGrouper mergeGrouper = new MergeGrouper(ctx, mergeKeyFields, mergeDecorFields, framesLimit,
                    comparatorFactories, firstKeyNormalizerFactory, partialMergerFactory, finalMergerFactory,
                    outRecordDesc, outRecordDesc);
            mergeGrouper.process(runs, outputWriter);
        }

    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

}
