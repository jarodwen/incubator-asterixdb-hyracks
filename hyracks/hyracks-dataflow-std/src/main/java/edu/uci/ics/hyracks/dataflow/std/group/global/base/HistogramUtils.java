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

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;

public class HistogramUtils {

    public static final int HISTOGRAM_SLOTS = 20;

    private static final int HISTOGRAM_BUCKET_HASH_SEED = 239;

    private static final int PRIME_SEED = 31;

    private static final IBinaryHashFunction histogramBucketHashFunction = MurmurHash3BinaryHashFunctionFamily.INSTANCE
            .createBinaryHashFunction(HISTOGRAM_BUCKET_HASH_SEED);

    public static int getHistogramBucketID(IFrameTupleAccessor frameTupleAccessor, int tupleIndex, int[] fields)
            throws HyracksDataException {
        int h = 0;
        int startOffset = frameTupleAccessor.getTupleStartOffset(tupleIndex);
        int slotLength = frameTupleAccessor.getFieldSlotsLength();
        for (int f : fields) {
            int fieldStart = frameTupleAccessor.getFieldStartOffset(tupleIndex, f);
            int fieldEnd = frameTupleAccessor.getFieldEndOffset(tupleIndex, f);
            h = h
                    * PRIME_SEED
                    + histogramBucketHashFunction.hash(frameTupleAccessor.getBuffer().array(), startOffset + slotLength
                            + fieldStart, fieldEnd - fieldStart);
        }
        if (h < 0) {
            h = -(h + 1);
        }
        return h % HISTOGRAM_SLOTS;
    }
}
