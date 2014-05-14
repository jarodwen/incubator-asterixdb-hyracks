package edu.uci.ics.hyracks.dataflow.std.group.global.data;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

public class HashFunctionFamilyFactoryAdapter {

    public static IBinaryHashFunctionFactory getFunctionFactoryFromFunctionFamily(
            final IBinaryHashFunctionFamily functionFamily, final int seed) {
        return new IBinaryHashFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IBinaryHashFunction createBinaryHashFunction() {
                return functionFamily.createBinaryHashFunction(seed);
            }
        };
    }

}
