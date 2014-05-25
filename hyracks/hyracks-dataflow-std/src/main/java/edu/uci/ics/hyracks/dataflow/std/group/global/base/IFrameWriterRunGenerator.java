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
package edu.uci.ics.hyracks.dataflow.std.group.global.base;

import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;

public interface IFrameWriterRunGenerator extends IFrameWriter {

    /**
     * Get the reader handlers for the output run files.
     * 
     * @return
     * @throws HyracksDataException
     */
    List<RunFileReader> getOutputRunReaders() throws HyracksDataException;

    /**
     * Get the total number of output run files.
     * 
     * @return
     */
    int getRunsCount();

    /**
     * Get the number of rows in each output run file.
     * 
     * @return
     * @throws HyracksDataException
     */
    List<Long> getOutputRunSizeInRows() throws HyracksDataException;

    /**
     * Get the number of groups (estimated) in each output run file.
     * @return
     * @throws HyracksDataException
     */
    List<Long> getOutputGroupsInRows() throws HyracksDataException;

    /**
     * Get the number of rows those are fully aggregated (for the given input partition, but not necessarily for the
     * global aggregation result).
     * 
     * @return
     */
    long getRecordsCompletelyAggregated();

    /**
     * Get the number of groups those are fully aggregated (for the given input partition, but not necessarily for the
     * global aggregation result).
     * 
     * @return
     */
    long getGroupsCompletelyAggregated();

    /**
     * Wrap up function to finish the run generation.
     * 
     * @throws HyracksDataException
     */
    void wrapup() throws HyracksDataException;
}
