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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.DataOutputWrapperForByteArray;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory;

public class DoubleSumAggregatorFactory implements IAggregatorFactory {

    private static final int DOUBLE_SIZE_IN_BYTES = 8;

    private final int doubleFieldToSum;

    public DoubleSumAggregatorFactory(int doubleFieldToSum) {
        this.doubleFieldToSum = doubleFieldToSum;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory#createAggregator()
     */
    @Override
    public IAggregator createAggregator() throws HyracksDataException {
        return new IAggregator() {

            private final DataOutputWrapperForByteArray wrapperOutput = new DataOutputWrapperForByteArray();

            @Override
            public boolean useDynamicState() {
                return false;
            }

            @Override
            public void step(IFrameTupleAccessor accessor, int tupleIndex, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                double currentValue = DoubleSerializerDeserializer.getDouble(fixedLengthState.getByteArray(),
                        fixedLengthState.getStartOffset());
                int offset = accessor.getTupleStartOffset(tupleIndex)
                        + accessor.getFieldStartOffset(tupleIndex, doubleFieldToSum);
                currentValue += DoubleSerializerDeserializer.getDouble(accessor.getBuffer().array(), offset);
                wrapperOutput.reset(fixedLengthState);
                try {
                    wrapperOutput.writeDouble(currentValue);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void restoreState(DataInput dumpedStateInput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    double currentValue = dumpedStateInput.readDouble();
                    wrapperOutput.reset(fixedLengthState);
                    wrapperOutput.writeDouble(currentValue);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void output(List<Integer> outputOffsets, DataOutput output, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    double currentValue = DoubleSerializerDeserializer.getDouble(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset());
                    output.writeDouble(currentValue);
                    outputOffsets.add(DOUBLE_SIZE_IN_BYTES);
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
                return DOUBLE_SIZE_IN_BYTES;
            }

            @Override
            public void dumpState(DataOutput dumpOutput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    dumpOutput.writeDouble(DoubleSerializerDeserializer.getDouble(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset()));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
