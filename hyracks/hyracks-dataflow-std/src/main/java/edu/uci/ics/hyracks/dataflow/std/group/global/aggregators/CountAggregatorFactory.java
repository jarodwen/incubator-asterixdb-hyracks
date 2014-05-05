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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.DataOutputWrapperForByteArray;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory;

public class CountAggregatorFactory implements IAggregatorFactory {

    public static final IAggregatorFactory INSTANCE = new CountAggregatorFactory();

    public static final int LONG_SIZE_IN_BYTES = 8;

    private CountAggregatorFactory() {
    }

    public IAggregator createAggregator() {
        return new IAggregator() {

            private final DataOutputWrapperForByteArray wrapperOutput = new DataOutputWrapperForByteArray();

            /*
             * (non-Javadoc)
             * 
             * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator#init()
             */
            @Override
            public void init() throws HyracksDataException {
                // do nothing
            }

            /*
             * (non-Javadoc)
             * 
             * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator#getRequiredSizeForFixedLengthState()
             */
            @Override
            public int getRequiredSizeForFixedLengthState() {
                // use int64 for the count
                return LONG_SIZE_IN_BYTES;
            }

            /*
             * (non-Javadoc)
             * 
             * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator#useDynamicState()
             */
            @Override
            public boolean useDynamicState() {
                // no dynamic state is required
                return false;
            }

            /*
             * (non-Javadoc)
             * 
             * @see
             * edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator#step(edu.uci.ics.hyracks.api.comm.
             * IFrameTupleAccessor
             * , int, edu.uci.ics.hyracks.data.std.api.IValueReference,
             * edu.uci.ics.hyracks.data.std.api.IMutableValueStorage)
             */
            @Override
            public void step(IFrameTupleAccessor accessor, int tupleIndex, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                long currentCount = Integer64SerializerDeserializer.getLong(fixedLengthState.getByteArray(),
                        fixedLengthState.getStartOffset());
                wrapperOutput.reset(fixedLengthState);
                try {
                    wrapperOutput.writeLong(currentCount + 1);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            /*
             * (non-Javadoc)
             * 
             * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator#dumpState(java.io.DataOutput,
             * edu.uci.ics.hyracks.data.std.api.IValueReference, edu.uci.ics.hyracks.data.std.api.IMutableValueStorage)
             */
            @Override
            public void dumpState(DataOutput dumpOutput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    dumpOutput.writeLong(Integer64SerializerDeserializer.getLong(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset()));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            /*
             * (non-Javadoc)
             * 
             * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator#restoreState(java.io.DataInput,
             * edu.uci.ics.hyracks.data.std.api.IValueReference, edu.uci.ics.hyracks.data.std.api.IMutableValueStorage)
             */
            @Override
            public void restoreState(DataInput dumpedStateInput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    long currentCount = dumpedStateInput.readLong();
                    wrapperOutput.reset(fixedLengthState);
                    wrapperOutput.writeLong(currentCount);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            /*
             * (non-Javadoc)
             * 
             * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator#output(java.io.DataOutput,
             * edu.uci.ics.hyracks.data.std.api.IValueReference, edu.uci.ics.hyracks.data.std.api.IMutableValueStorage)
             */
            @Override
            public void output(List<Integer> outputOffsets, DataOutput output, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                try {
                    long currentCount = Integer64SerializerDeserializer.getLong(fixedLengthState.getByteArray(),
                            fixedLengthState.getStartOffset());
                    output.writeLong(currentCount);
                    outputOffsets.add(LONG_SIZE_IN_BYTES);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
