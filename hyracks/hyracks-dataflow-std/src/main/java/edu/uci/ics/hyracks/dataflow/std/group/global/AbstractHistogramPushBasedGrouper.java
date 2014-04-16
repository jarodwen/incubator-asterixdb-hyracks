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

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor.GroupOutputState;

public abstract class AbstractHistogramPushBasedGrouper implements IFrameWriter {

    public static final int INT_SIZE = 4;

    protected final IHyracksTaskContext ctx;

    protected final int[] keyFields, decorFields;

    protected final int framesLimit;

    protected final int frameSize;

    protected final IFrameWriter outputWriter;

    protected final IAggregatorDescriptorFactory aggregatorFactory, mergerFactory;

    protected final RecordDescriptor inRecDesc, outRecDesc;

    protected final List<RunFileReader> runReaders;

    protected final boolean isGenerateRuns;

    public AbstractHistogramPushBasedGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields,
            int framesLimit, IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            boolean enableHistorgram, IFrameWriter outputWriter, boolean isGenerateRuns) {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.frameSize = ctx.getFrameSize();
        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;
        this.outputWriter = outputWriter;
        this.isGenerateRuns = isGenerateRuns;

        this.runReaders = new LinkedList<RunFileReader>();
    }

    abstract protected void reset() throws HyracksDataException;

    abstract protected void flush(IFrameWriter writer, GroupOutputState outputStateType) throws HyracksDataException;

    abstract protected void dumpAndCleanDebugCounters();

    public List<RunFileReader> getOutputRunReaders() throws HyracksDataException {
        return this.runReaders;
    }

    public int getRunsCount() {
        return this.runReaders.size();
    }

    public void wrapup() throws HyracksDataException {
        // flush the records if there are any left in the memory
        if (isGenerateRuns && runReaders.size() > 0) {
            IFrameWriter dumpWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(SortGrouper.class
                    .getSimpleName()), ctx.getIOManager());
            dumpWriter.open();
            flush(dumpWriter, GroupOutputState.GROUP_STATE);
            RunFileReader runReader = ((RunFileWriter) dumpWriter).createReader();
            this.runReaders.add(runReader);
            dumpWriter.close();
        } else {
            flush(outputWriter, GroupOutputState.RESULT_STATE);
        }
    }

}
