package edu.uci.ics.hyracks.dataflow.std.group.global.base;

import java.util.HashMap;

import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;

public class OperatorDebugCounterCollection {

    private final static String PREFIX_REQUIRED = ".required";
    private final static String PREFIX_OPTIONAL = ".optional";
    private final static String PREFIX_COMMON = ".common";
    private final static String PREFIX_SORT = ".sort";
    private final static String PREFIX_HASH = ".hash";
    private final static String PREFIX_CUSTOMIZER = ".cust";

    private final String debugID;

    /**
     * Common required cost factors
     */
    public enum RequiredCounters {

        CPU(".cpu"),
        IO_OUT_DISK(".io.out.disk"),
        IO_OUT_NETWORK(".io.out.network"),
        IO_IN_DISK(".io.in.disk"),
        IO_IN_NETWORK(".io.in.network"),
        IN_FRAMES(".in.frames"),
        IN_RECOEDS(".in.records"),
        OUT_FRAMES(".out.frames"),
        OUT_RECORDS(".out.records");

        private final String counterName;

        private RequiredCounters(
                String counterName) {
            this.counterName = PREFIX_REQUIRED + counterName;
        }

        public String getName() {
            return counterName;
        }
    }

    private final long[] requiredCounters = new long[RequiredCounters.values().length];

    /**
     * Common counters
     */
    public enum OptionalCommonCounters {

        RECORD_INPUT(".record.input"),
        RECORD_OUTPUT(".record.output"),
        FRAME_INPUT(".frame.input"),
        FRAME_OUTPUT(".frame.output"),
        RUN_GENERATED(".run.generated");

        private final String counterName;

        private OptionalCommonCounters(
                String counterName) {
            this.counterName = PREFIX_OPTIONAL + PREFIX_COMMON + counterName;
        }

        public String getName() {
            return counterName;
        }
    }

    private final long[] optionalCommonCounters = new long[OptionalCommonCounters.values().length];

    /**
     * Sort-related counters
     */
    public enum OptionalSortCounters {

        CPU_COMPARE(".cpu.compare"),
        CPU_COPY(".cpu.copy");

        private final String counterName;

        private OptionalSortCounters(
                String counterName) {
            this.counterName = PREFIX_OPTIONAL + PREFIX_SORT + counterName;
        }

        public String getName() {
            return counterName;
        }
    }

    private final long[] optionalSortCounters = new long[OptionalSortCounters.values().length];

    /**
     * Hash-related counters
     */
    public enum OptionalHashCounters {

        CPU_COMPARE(".cpu.compare"),
        CPU_COMPARE_HIT(".cpu.compare.hit"),
        CPU_COMPARE_MISS(".cpu.compare.miss"),
        HITS(".hits"),
        MISSES(".misses");

        private final String counterName;

        private OptionalHashCounters(
                String counterName) {
            this.counterName = PREFIX_OPTIONAL + PREFIX_HASH + counterName;
        }

        public String getName() {
            return counterName;
        }
    }

    private final long[] optionalHashCounters = new long[OptionalHashCounters.values().length];

    private final HashMap<String, Long> optionalCustomizedCounters = new HashMap<>();

    public OperatorDebugCounterCollection(
            String debugID) {

        this.debugID = debugID;

    }

    public void reset() {
        int i;
        for (i = 0; i < requiredCounters.length; i++) {
            requiredCounters[i] = 0;
        }
        for (i = 0; i < optionalCommonCounters.length; i++) {
            optionalCommonCounters[i] = 0;
        }
        for (i = 0; i < optionalSortCounters.length; i++) {
            optionalSortCounters[i] = 0;
        }
        for (i = 0; i < optionalHashCounters.length; i++) {
            optionalHashCounters[i] = 0;
        }
        // remove all customized counters
        optionalCustomizedCounters.clear();
    }

    public void dumpCounters(
            ICounterContext counterCtx) {
        for (RequiredCounters c : RequiredCounters.values()) {
            counterCtx.getCounter(debugID + c.counterName, true).update(requiredCounters[c.ordinal()]);
        }
        for (OptionalCommonCounters c : OptionalCommonCounters.values()) {
            counterCtx.getCounter(debugID + c.counterName, true).update(optionalCommonCounters[c.ordinal()]);
        }
        for (OptionalSortCounters c : OptionalSortCounters.values()) {
            counterCtx.getCounter(debugID + c.counterName, true).update(optionalSortCounters[c.ordinal()]);
        }
        for (OptionalHashCounters c : OptionalHashCounters.values()) {
            counterCtx.getCounter(debugID + c.counterName, true).update(optionalHashCounters[c.ordinal()]);
        }
        for (String c : optionalCustomizedCounters.keySet()) {
            counterCtx.getCounter(debugID + c, true).update(optionalCustomizedCounters.get(c));
        }
    }

    public void updateRequiredCounter(
            RequiredCounters rcName,
            long delta) {
        requiredCounters[rcName.ordinal()] += delta;
    }

    public void updateOptionalCommonCounter(
            OptionalCommonCounters ccName,
            long delta) {
        optionalCommonCounters[ccName.ordinal()] += delta;
    }

    public void updateOptionalSortCounter(
            OptionalSortCounters scName,
            long delta) {
        optionalSortCounters[scName.ordinal()] += delta;
    }

    public void updateOptionalHashCounter(
            OptionalHashCounters hcName,
            long delta) {
        optionalHashCounters[hcName.ordinal()] += delta;
    }

    public void updateOptionalCustomizedCounter(
            String ccName,
            long delta) {
        String fullKeyName = PREFIX_OPTIONAL + PREFIX_CUSTOMIZER + ccName;
        long val = (optionalCustomizedCounters.containsKey(fullKeyName) ? optionalCustomizedCounters.get(fullKeyName)
                : 0);
        optionalCustomizedCounters.put(fullKeyName, val + delta);
    }

    public String getDebugID() {
        return this.debugID;
    }
}
