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
package edu.uci.ics.hyracks.dataflow.std.group.global.aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory;

public class StringMinMaxAggregatorFactory implements IAggregatorFactory {

    private final int stringField;
    private final boolean isMin;

    public StringMinMaxAggregatorFactory(int stringField, boolean isMin) {
        this.stringField = stringField;
        this.isMin = isMin;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory#createAggregator()
     */
    @Override
    public IAggregator createAggregator() throws HyracksDataException {
        return new IAggregator() {

            @Override
            public boolean useDynamicState() {
                return true;
            }

            @Override
            public void step(IFrameTupleAccessor accessor, int tupleIndex, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                int currentLength = UTF8StringPointable.getUTFLength(dynamicState.getByteArray(),
                        dynamicState.getStartOffset());
                int offset = accessor.getTupleStartOffset(tupleIndex)
                        + accessor.getFieldStartOffset(tupleIndex, stringField);
                int length = accessor.getFieldLength(tupleIndex, stringField);
                int newStringLength = UTF8StringPointable.getUTFLength(accessor.getBuffer().array(), offset);
                boolean toReplace = (isMin) ? (newStringLength < currentLength) : (newStringLength > currentLength);
                if (toReplace) {
                    try {
                        dynamicState.getDataOutput().write(accessor.getBuffer().array(), offset, length);
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void restoreState(DataInput dumpedStateInput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    dynamicState.getDataOutput().writeUTF(dumpedStateInput.readUTF());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void output(List<Integer> outputOffsets, DataOutput output, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    output.write(dynamicState.getByteArray(), dynamicState.getStartOffset(), dynamicState.getLength());
                    outputOffsets.add(dynamicState.getLength());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void init() throws HyracksDataException {
                // do nothing
            }

            @Override
            public int getRequiredSizeForFixedLengthState() {
                // the space that may be used by the grouper for indexing the dynamic state
                return 8;
            }

            @Override
            public void dumpState(DataOutput dumpOutput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    dumpOutput.write(dynamicState.getByteArray(), dynamicState.getStartOffset(),
                            dynamicState.getLength());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
