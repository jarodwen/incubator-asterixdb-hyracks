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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class HashGroupSortMergeGrouper implements IFrameWriter {

    private HashGrouper hashGrouper;

    protected final IHyracksTaskContext ctx;
    protected final int[] keyFields;
    protected final int[] decorFields;
    private final int framesLimit;
    private final int tableSize;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;
    private final RecordDescriptor inRecordDesc, outRecordDesc;
    private final IFrameWriter outputWriter;

    public HashGroupSortMergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            int tableSize, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.hashFunctionFactories = hashFunctionFactories;
        this.inRecordDesc = inRecordDescriptor;
        this.outRecordDesc = outRecordDescriptor;
        this.outputWriter = outputWriter;

        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;
    }

    @Override
    public void open() throws HyracksDataException {
        this.hashGrouper = new HashGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                finalMergerFactory, inRecordDesc, outRecordDesc, false, outputWriter, true, tableSize,
                comparatorFactories, hashFunctionFactories, firstKeyNormalizerFactory);
        this.hashGrouper.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        this.hashGrouper.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        hashGrouper.wrapup();
        if (hashGrouper.getRunsCount() == 0) {
            hashGrouper.close();
        } else {
            List<RunFileReader> runs = hashGrouper.getOutputRunReaders();
            hashGrouper.close();
            int[] mergeKeyFields = new int[keyFields.length];
            for (int i = 0; i < mergeKeyFields.length; i++) {
                mergeKeyFields[i] = i;
            }
            int[] mergeDecorFields = new int[decorFields.length];
            for (int i = 0; i < decorFields.length; i++) {
                mergeDecorFields[i] = keyFields.length + i;
            }
            MergeGrouper mergeGrouper = new MergeGrouper(ctx, mergeKeyFields, mergeDecorFields, framesLimit, tableSize,
                    comparatorFactories, firstKeyNormalizerFactory, hashFunctionFactories, partialMergerFactory,
                    finalMergerFactory, outRecordDesc, outRecordDesc);
            mergeGrouper.process(runs, outputWriter);
        }
    }

    @Override
    public void fail() throws HyracksDataException {

    }

}
