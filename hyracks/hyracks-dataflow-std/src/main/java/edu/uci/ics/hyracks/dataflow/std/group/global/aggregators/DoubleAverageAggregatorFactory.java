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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.DataOutputWrapperForByteArray;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory;

public class DoubleAverageAggregatorFactory implements IAggregatorFactory {

    private final int doubleField;
    private static final int DOUBLE_SIZE_IN_BYTES = 8;
    private static final int LONG_SIZE_IN_BYTES = 8;

    public DoubleAverageAggregatorFactory(int doubleField) {
        this.doubleField = doubleField;
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

                double currentSum = DoubleSerializerDeserializer.getDouble(fixedLengthState.getByteArray(),
                        fixedLengthState.getStartOffset());
                long currentCount = Integer64SerializerDeserializer.getLong(fixedLengthState.getByteArray(),
                        fixedLengthState.getStartOffset() + DOUBLE_SIZE_IN_BYTES);
                int offset = accessor.getTupleStartOffset(tupleIndex)
                        + accessor.getFieldStartOffset(tupleIndex, doubleField);
                currentSum += DoubleSerializerDeserializer.getDouble(accessor.getBuffer().array(), offset);
                currentCount++;
                wrapperOutput.reset(fixedLengthState);
                try {
                    wrapperOutput.writeDouble(currentSum);
                    wrapperOutput.writeLong(currentCount);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void restoreState(DataInput dumpedStateInput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    double currentSum = dumpedStateInput.readDouble();
                    long currentCount = dumpedStateInput.readLong();
                    wrapperOutput.reset(fixedLengthState);
                    wrapperOutput.writeDouble(currentSum);
                    wrapperOutput.writeLong(currentCount);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void output(List<Integer> outputOffsets, DataOutput output, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    double currentSum = DoubleSerializerDeserializer.getDouble(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset());
                    long currentCount = Integer64SerializerDeserializer.getLong(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset() + DOUBLE_SIZE_IN_BYTES);
                    output.writeDouble(currentSum / currentCount);
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
                return DOUBLE_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES;
            }

            @Override
            public void dumpState(DataOutput dumpOutput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    dumpOutput.writeDouble(DoubleSerializerDeserializer.getDouble(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset()));
                    dumpOutput.writeLong(Integer64SerializerDeserializer.getLong(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset() + DOUBLE_SIZE_IN_BYTES));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

}
