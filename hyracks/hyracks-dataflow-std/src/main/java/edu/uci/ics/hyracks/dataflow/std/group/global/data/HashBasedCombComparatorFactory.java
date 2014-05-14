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
package edu.uci.ics.hyracks.dataflow.std.group.global.data;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public class HashBasedCombComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    private final IPointableFactory pf;
    private final IBinaryHashFunctionFactory hf;

    public HashBasedCombComparatorFactory(IBinaryHashFunctionFactory hf, IPointableFactory pf) {
        this.hf = hf;
        this.pf = pf;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory#createBinaryComparator()
     */
    @Override
    public IBinaryComparator createBinaryComparator() {
        final IPointable p = pf.createPointable();
        final IBinaryHashFunction h = hf.createBinaryHashFunction();
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                int h1 = h.hash(b1, s1, l1);
                int h2 = h.hash(b2, s2, l2);
                if (h1 == h2) {
                    p.set(b1, s1, l1);
                    return ((IComparable) p).compareTo(b2, s2, l2);
                }
                return (h1 > h2) ? 1 : -1;
            }
        };
    }

}
