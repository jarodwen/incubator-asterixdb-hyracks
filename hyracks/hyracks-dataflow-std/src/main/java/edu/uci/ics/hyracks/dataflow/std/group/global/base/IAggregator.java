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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public interface IAggregator {

    void init() throws HyracksDataException;

    int getRequiredSizeForFixedLengthState();

    boolean useDynamicState();

    void step(IFrameTupleAccessor accessor, int tupleIndex, IValueReference fixedLengthState,
            IMutableValueStorage dynamicState) throws HyracksDataException;

    void dumpState(DataOutput dumpOutput, IValueReference fixedLengthState, IMutableValueStorage dynamicState)
            throws HyracksDataException;

    void restoreState(DataInput dumpedStateInput, IValueReference fixedLengthState, IMutableValueStorage dynamicState)
            throws HyracksDataException;

    void output(List<Integer> outputOffsets, DataOutput output, IValueReference fixedLengthState, IMutableValueStorage dynamicState)
            throws HyracksDataException;

}
