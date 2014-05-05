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
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.ValueReferenceWithOffset;

public class MultiFieldAggregatorFactory implements IAggregatorFactory {

    private final IAggregatorFactory[] aggregatorFactories;

    public MultiFieldAggregatorFactory(IAggregatorFactory[] aggregatorFactories) {
        this.aggregatorFactories = aggregatorFactories;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IAggregatorFactory#createAggregator()
     */
    @Override
    public IAggregator createAggregator() throws HyracksDataException {
        return new IAggregator() {

            private IAggregator[] aggregators;
            private ValueReferenceWithOffset valRefWithOffset;

            @Override
            public boolean useDynamicState() {
                for (IAggregator aggregator : aggregators) {
                    if (aggregator.useDynamicState()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void step(IFrameTupleAccessor accessor, int tupleIndex, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                int fieldOffset = 0;
                for (int i = 0; i < aggregators.length; i++) {
                    valRefWithOffset.reset(fixedLengthState, fieldOffset);
                    aggregators[i].step(accessor, tupleIndex, valRefWithOffset, dynamicState);
                    fieldOffset += aggregators[i].getRequiredSizeForFixedLengthState();
                }
            }

            @Override
            public void restoreState(DataInput dumpedStateInput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                int fieldOffset = 0;
                for (int i = 0; i < aggregators.length; i++) {
                    valRefWithOffset.reset(fixedLengthState, fieldOffset);
                    aggregators[i].restoreState(dumpedStateInput, valRefWithOffset, dynamicState);
                }
            }

            @Override
            public void output(List<Integer> outputOffsets, DataOutput output, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                int fieldOffset = 0;
                for (int i = 0; i < aggregators.length; i++) {
                    valRefWithOffset.reset(fixedLengthState, fieldOffset);
                    aggregators[i].output(outputOffsets, output, valRefWithOffset, dynamicState);
                }
            }

            @Override
            public void init() throws HyracksDataException {
                aggregators = new IAggregator[aggregatorFactories.length];
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i] = aggregatorFactories[i].createAggregator();
                }
                valRefWithOffset = new ValueReferenceWithOffset();
            }

            @Override
            public int getRequiredSizeForFixedLengthState() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public void dumpState(DataOutput dumpOutput, IValueReference fixedLengthState,
                    IMutableValueStorage dynamicState) throws HyracksDataException {
                // TODO Auto-generated method stub

            }
        };
    }

}
