package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.GrouperFlushOption;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IGrouperFlushOption.GroupOutputState;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalCommonCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalHashCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.OptionalSortCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.OperatorDebugCounterCollection.RequiredCounters;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashTableFrameTupleAppender;

/**
 * Hash grouper uses an internal hash table (linked-list-chain based) to support the group
 * lookup for the aggregation. Records are aggregated inside of the hash table until the
 * hash table frames are full (no space for new groups). Then the grouper can be used to
 * either dump the hash table content directly to the next operator, or sort the records
 * in (hash_id, keys) order before dump.
 */
public class HashGrouper extends AbstractHistogramPushBasedGrouper {

    protected static final int LIST_FRAME_REF_SIZE = 4;
    protected static final int LIST_TUPLE_REF_SIZE = 4;
    protected static final int POINTER_INIT_SIZE = 8;
    protected static final int POINTER_LENGTH = 3;

    protected final int tableSize;

    private final IAggregatorDescriptor aggregator, merger;
    private AggregateState aggState;

    private final INormalizedKeyComputer firstNormalizerComputer;
    private final IBinaryComparator[] comparators;

    protected ByteBuffer[] headers;
    protected ByteBuffer[] contents;

    private ByteBuffer outputBuffer;

    private FrameTupleAccessor inputFrameTupleAccessor, compFrameAccessor, hashtableFrameAccessor;

    private FrameTupleAppender outputAppender;

    private HashTableFrameTupleAppender hashtableFrameTupleAppender;

    private ArrayTupleBuilder hashtableGroupTupleBuilder, outputTupleBuilder;

    private int[] tPointers;

    private int processedTuple;

    private int groupsInHashtable;

    /**
     * Used for hash table lookup, to maintain the pointer to the group in the hash table.
     */
    private int lookupFrameIndex, lookupTupleIndex;

    /**
     * For computing the slot value for a given tuple
     */
    private ITuplePartitionComputer tuplePartitionComputer;

    private int currentWorkingFrame;

    private boolean isAllInputProcessed = false;

    private long debugOptionalHashHits = 0, debugOptionalHashMisses = 0, debugOptionalCPUCompareHit = 0,
            debugOptionalCPUCompareMiss = 0, debugRequiredCPU = 0, debugOptionalSortCPUCompare = 0,
            debugOptionalSortCPUCopy = 0, debugOptionalIOStreamed = 0, debugOptionalIODumped = 0;
    private double debugOptionalMaxHashtableFillRatio = 0;
    private long debugTempCPUCounter = 0, debugTempGroupsInHashtable = 0, debugTempUsedSlots = 0;

    private long profileCPU, profileIOInNetwork, profileIOInDisk, profileIOOutDisk, profileIOOutNetwork;

    private long profileInRecords, profileInFrames, profileOutRecords, profileOutFrames;

    public HashGrouper(
            IHyracksTaskContext ctx,
            int[] keyFields,
            int[] decorFields,
            int framesLimit,
            IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc,
            boolean enableHistorgram,
            IFrameWriter outputWriter,
            boolean isGenerateRuns,
            int tableSize,
            IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFactory[] hashFunctionFactories,
            INormalizedKeyComputerFactory firstNormalizerComputerFactory) throws HyracksDataException {
        super(ctx, keyFields, decorFields, framesLimit, aggregatorFactory, mergerFactory, inRecDesc, outRecDesc,
                enableHistorgram, outputWriter, isGenerateRuns);

        this.tableSize = tableSize;

        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < this.comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.tuplePartitionComputer = new FieldHashPartitionComputerFactory(keyFields, hashFunctionFactories)
                .createPartitioner();
        this.firstNormalizerComputer = firstNormalizerComputerFactory == null ? null : firstNormalizerComputerFactory
                .createNormalizedKeyComputer();

        int[] storedKeys = new int[keyFields.length];

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecDesc, outRecDesc, keyFields, storedKeys, null);
        this.aggState = aggregator.createAggregateStates();
        this.merger = mergerFactory.createAggregator(ctx, outRecDesc, outRecDesc, storedKeys, storedKeys, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#init()
     */
    @Override
    public void open() throws HyracksDataException {

        // initialize the hash table
        // - headers
        int headerFramesCount = (int) (Math.ceil((double) tableSize * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE)
                / frameSize));
        if (framesLimit < headerFramesCount + 2) {
            throw new HyracksDataException("Not enough frame (" + framesLimit + ") for a hash table with " + tableSize
                    + " slots.");
        }
        this.headers = new ByteBuffer[headerFramesCount];
        for (int i = 0; i < headers.length; i++) {
            this.headers[i] = ctx.allocateFrame();
        }
        resetHeaders();
        // - list storage area
        this.contents = new ByteBuffer[framesLimit - 1 - headers.length];
        for (int i = 0; i < contents.length; i++) {
            this.contents[i] = ctx.allocateFrame();
        }

        // initialize the accessors and appenders
        this.inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inRecDesc);
        this.hashtableFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);
        this.compFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        this.hashtableGroupTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());

        this.hashtableFrameTupleAppender = new HashTableFrameTupleAppender(frameSize, LIST_FRAME_REF_SIZE
                + LIST_TUPLE_REF_SIZE);

        // reset the hash table content frame
        this.currentWorkingFrame = 0;

        // reset the lookup reference
        this.lookupFrameIndex = -1;
        this.lookupTupleIndex = -1;

        resetHistogram();
    }

    /**
     * Try to lookup the hash table for possible match, and if there is a match, do aggregation;
     * otherwise a new group will be inserted into the hash table.
     */
    @Override
    public void nextFrame(
            ByteBuffer buffer) throws HyracksDataException {

        profileInFrames++;
        profileInRecords += buffer.getInt(buffer.capacity() - INT_SIZE);

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_INPUT, 1);

        profileIOInNetwork++;

        // reset the processed tuple count
        this.processedTuple = 0;

        inputFrameTupleAccessor.reset(buffer);

        int tupleCount = inputFrameTupleAccessor.getTupleCount();

        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_INPUT, tupleCount);

        int tupleIndex = 0;

        while (tupleIndex < tupleCount) {

            int h = tuplePartitionComputer.partition(inputFrameTupleAccessor, tupleIndex, tableSize);

            if (findMatch(inputFrameTupleAccessor, tupleIndex, h)) {
                // match found: do aggregation
                this.hashtableFrameAccessor.reset(contents[lookupFrameIndex]);
                int tupleStartOffset = this.hashtableFrameAccessor.getTupleStartOffset(lookupTupleIndex);
                int tupleEndOffset = this.hashtableFrameAccessor.getTupleEndOffset(lookupTupleIndex);
                this.aggregator.aggregate(inputFrameTupleAccessor, tupleIndex, contents[lookupFrameIndex].array(),
                        tupleStartOffset, tupleEndOffset - tupleStartOffset, aggState);
            } else {
                // not found: try to create a new group in the hash table
                this.hashtableGroupTupleBuilder.reset();

                for (int i : keyFields) {
                    hashtableGroupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                for (int i : decorFields) {
                    hashtableGroupTupleBuilder.addField(inputFrameTupleAccessor, tupleIndex, i);
                }

                aggregator.init(hashtableGroupTupleBuilder, inputFrameTupleAccessor, tupleIndex, aggState);

                // insert the new group into the beginning of the slot
                getSlotPointer(h);

                if (lookupFrameIndex < 0) {
                    debugTempUsedSlots++;
                }

                hashtableFrameTupleAppender.reset(contents[currentWorkingFrame], false);
                if (!hashtableFrameTupleAppender.append(hashtableGroupTupleBuilder.getFieldEndOffsets(),
                        hashtableGroupTupleBuilder.getByteArray(), 0, hashtableGroupTupleBuilder.getSize(),
                        lookupFrameIndex, lookupTupleIndex)) {
                    currentWorkingFrame++;
                    if (currentWorkingFrame >= contents.length) {
                        // hash table is full: need to flush (to either next operator or a run file)

                        debugOptionalMaxHashtableFillRatio = Math.max(debugOptionalMaxHashtableFillRatio,
                                debugTempGroupsInHashtable / debugTempUsedSlots);
                        debugTempGroupsInHashtable = 0;
                        debugTempUsedSlots = 0;

                        if (isGenerateRuns) {
                            IFrameWriter dumpWriter = new RunFileWriter(
                                    ctx.createManagedWorkspaceFile(HashGrouper.class.getSimpleName()),
                                    ctx.getIOManager());
                            dumpWriter.open();
                            flush(dumpWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE);
                            RunFileReader runReader = ((RunFileWriter) dumpWriter).createReader();
                            this.runReaders.add(runReader);
                            dumpWriter.close();
                            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
                        } else {
                            flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE);
                        }
                        reset();
                    }
                    if (contents[currentWorkingFrame] == null) {
                        contents[currentWorkingFrame] = ctx.allocateFrame();
                    }
                    hashtableFrameTupleAppender.reset(contents[currentWorkingFrame], true);
                    if (!hashtableFrameTupleAppender.append(hashtableGroupTupleBuilder.getFieldEndOffsets(),
                            hashtableGroupTupleBuilder.getByteArray(), 0, hashtableGroupTupleBuilder.getSize(),
                            lookupFrameIndex, lookupTupleIndex)) {
                        throw new HyracksDataException(
                                "Failed to insert a group into the hash table: the record is too large.");
                    }
                }

                groupsInHashtable++;
                debugTempGroupsInHashtable++;

                // reset the header reference
                setSlotPointer(h, currentWorkingFrame, hashtableFrameTupleAppender.getTupleCount() - 1);
            }

            insertIntoHistogram(h);
            this.processedTuple++;

            tupleIndex++;
        }
    }

    private void setSlotPointer(
            int h,
            int contentFrameIndex,
            int contentTupleIndex) {
        int slotFrameIndex = (int) ((long) h * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE) / frameSize);
        int slotTupleOffset = (int) ((long) h * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE) % frameSize);

        headers[slotFrameIndex].putInt(slotTupleOffset, contentFrameIndex);
        headers[slotFrameIndex].putInt(slotTupleOffset + INT_SIZE, contentTupleIndex);
    }

    private void getSlotPointer(
            int h) {
        int slotFrameIndex = (int) ((long) h * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE) / frameSize);
        int slotTupleOffset = (int) ((long) h * (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE) % frameSize);

        lookupFrameIndex = headers[slotFrameIndex].getInt(slotTupleOffset);
        lookupTupleIndex = headers[slotFrameIndex].getInt(slotTupleOffset + INT_SIZE);
    }

    private boolean findMatch(
            FrameTupleAccessor accessor,
            int tupleIndex,
            int hashValue) throws HyracksDataException {
        getSlotPointer(hashValue);
        while (lookupFrameIndex >= 0) {
            hashtableFrameAccessor.reset(contents[lookupFrameIndex]);
            if (!sameGroup(accessor, tupleIndex, hashtableFrameAccessor, lookupTupleIndex)) {
                int tupleEndOffset = hashtableFrameAccessor.getTupleEndOffset(lookupTupleIndex);
                lookupFrameIndex = hashtableFrameAccessor.getBuffer().getInt(
                        tupleEndOffset - (LIST_FRAME_REF_SIZE + LIST_TUPLE_REF_SIZE));
                lookupTupleIndex = hashtableFrameAccessor.getBuffer().getInt(tupleEndOffset - LIST_TUPLE_REF_SIZE);
            } else {
                debugOptionalCPUCompareHit += debugTempCPUCounter;
                debugTempCPUCounter = 0;
                debugOptionalHashHits++;
                return true;
            }
        }
        debugOptionalHashMisses++;
        debugOptionalCPUCompareMiss += debugTempCPUCounter;
        debugTempCPUCounter = 0;
        return false;
    }

    protected boolean sameGroup(
            FrameTupleAccessor a1,
            int t1Idx,
            FrameTupleAccessor a2,
            int t2Idx) {
        debugTempCPUCounter++;
        debugRequiredCPU++;
        profileCPU++;
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = keyFields[i];
            int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength() + a1.getFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength() + a2.getFieldStartOffset(t2Idx, i);
            int l2 = a2.getFieldLength(t2Idx, i);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    private int sortEntry(
            int hashtableEntryID) {
        if (tPointers == null) {
            tPointers = new int[POINTER_INIT_SIZE * POINTER_LENGTH];
        }
        int ptr = 0;

        getSlotPointer(hashtableEntryID);

        while (lookupFrameIndex >= 0) {

            tPointers[ptr * POINTER_LENGTH] = lookupFrameIndex;
            tPointers[ptr * POINTER_LENGTH + 1] = lookupTupleIndex;

            hashtableFrameAccessor.reset(contents[lookupFrameIndex]);
            int tStart = hashtableFrameAccessor.getTupleStartOffset(lookupTupleIndex);
            int f0StartRel = hashtableFrameAccessor.getFieldStartOffset(lookupTupleIndex, 0);
            int f0EndRel = hashtableFrameAccessor.getFieldEndOffset(lookupTupleIndex, 0);
            int f0Start = f0StartRel + tStart + hashtableFrameAccessor.getFieldSlotsLength();
            tPointers[ptr * POINTER_LENGTH + 2] = firstNormalizerComputer == null ? 0 : firstNormalizerComputer
                    .normalize(hashtableFrameAccessor.getBuffer().array(), f0Start, f0EndRel - f0StartRel);
            ptr++;

            if (ptr * POINTER_LENGTH >= tPointers.length) {
                int[] newTPointers = new int[tPointers.length * 2];
                System.arraycopy(tPointers, 0, newTPointers, 0, tPointers.length);
                tPointers = newTPointers;
            }

            int tupleEndOffset = hashtableFrameAccessor.getTupleEndOffset(lookupTupleIndex);

            lookupFrameIndex = hashtableFrameAccessor.getBuffer().getInt(
                    tupleEndOffset - LIST_FRAME_REF_SIZE - LIST_TUPLE_REF_SIZE);
            lookupTupleIndex = hashtableFrameAccessor.getBuffer().getInt(tupleEndOffset - LIST_TUPLE_REF_SIZE);
        }

        if (ptr > 1) {
            sort(0, ptr);
        }

        return ptr;
    }

    protected void sort(
            int offset,
            int len) {
        int m = offset + (len >> 1);
        int mFrameIndex = tPointers[m * POINTER_LENGTH];
        int mTupleIndex = tPointers[m * POINTER_LENGTH + 1];
        int mNormKey = tPointers[m * POINTER_LENGTH + 2];
        hashtableFrameAccessor.reset(contents[mFrameIndex]);

        int a = offset;
        int b = a;
        int c = offset + len - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int bFrameIndex = tPointers[b * POINTER_LENGTH];
                int bTupleIndex = tPointers[b * POINTER_LENGTH + 1];
                int bNormKey = tPointers[b * POINTER_LENGTH + 2];
                int cmp = 0;
                if (bNormKey != mNormKey) {
                    cmp = ((((long) bNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    compFrameAccessor.reset(contents[bFrameIndex]);
                    cmp = compare(compFrameAccessor, bTupleIndex, hashtableFrameAccessor, mTupleIndex);
                }
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(a++, b);
                }
                ++b;
            }
            while (c >= b) {
                int cFrameIndex = tPointers[c * POINTER_LENGTH];
                int cTupleIndex = tPointers[c * POINTER_LENGTH + 1];
                int cNormKey = tPointers[c * POINTER_LENGTH + 2];
                int cmp = 0;
                if (cNormKey != mNormKey) {
                    cmp = ((((long) cNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    compFrameAccessor.reset(contents[cFrameIndex]);
                    cmp = compare(compFrameAccessor, cTupleIndex, hashtableFrameAccessor, mTupleIndex);
                }
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(c, d--);
                }
                --c;
            }
            if (b > c)
                break;
            swap(b++, c--);
        }

        int s;
        int n = offset + len;
        s = Math.min(a - offset, b - a);
        vecswap(offset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(b, n - s, s);

        if ((s = b - a) > 1) {
            sort(offset, s);
        }
        if ((s = d - c) > 1) {
            sort(n - s, s);
        }
    }

    private int compare(
            FrameTupleAccessor accessor1,
            int tupleIndex1,
            FrameTupleAccessor accessor2,
            int tupleIndex2) {
        debugOptionalSortCPUCompare++;
        debugRequiredCPU++;
        profileCPU++;

        int tStart1 = accessor1.getTupleStartOffset(tupleIndex1);
        int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

        int tStart2 = accessor2.getTupleStartOffset(tupleIndex2);
        int fStartOffset2 = accessor2.getFieldSlotsLength() + tStart2;

        for (int i = 0; i < keyFields.length; ++i) {
            int fStart1 = accessor1.getFieldStartOffset(tupleIndex1, i);
            int fEnd1 = accessor1.getFieldEndOffset(tupleIndex1, i);
            int fLen1 = fEnd1 - fStart1;

            int fStart2 = accessor2.getFieldStartOffset(tupleIndex2, i);
            int fEnd2 = accessor2.getFieldEndOffset(tupleIndex2, i);
            int fLen2 = fEnd2 - fStart2;

            int c = comparators[i].compare(accessor1.getBuffer().array(), fStart1 + fStartOffset1, fLen1, accessor2
                    .getBuffer().array(), fStart2 + fStartOffset2, fLen2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private void swap(
            int a,
            int b) {
        debugOptionalSortCPUCopy++;
        for (int i = 0; i < POINTER_LENGTH; i++) {
            int t = tPointers[a * POINTER_LENGTH + i];
            tPointers[a * POINTER_LENGTH + i] = tPointers[b * POINTER_LENGTH + i];
            tPointers[b * POINTER_LENGTH + i] = t;
        }
    }

    private void vecswap(
            int a,
            int b,
            int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(a, b);
        }
    }

    @Override
    protected void flush(
            IFrameWriter writer,
            GrouperFlushOption flushOption) throws HyracksDataException {

        IAggregatorDescriptor aggregatorToFlush = (flushOption.getOutputState() == GroupOutputState.RESULT_STATE) ? merger
                : aggregator;

        if (outputTupleBuilder == null) {
            outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        }

        if (outputAppender == null) {
            outputAppender = new FrameTupleAppender(frameSize);
        }

        if (outputBuffer == null) {
            outputBuffer = ctx.allocateFrame();
        }

        outputAppender.reset(outputBuffer, true);

        if (isGenerateRuns && !(isAllInputProcessed && this.runReaders.size() == 0)) {
            // output hash table contents in the order of (hash_value, keys)
            for (int i = 0; i < tableSize; i++) {
                int tupleInEntry = sortEntry(i);
                for (int ptr = 0; ptr < tupleInEntry; ptr++) {
                    int frameIndex = tPointers[ptr * POINTER_LENGTH];
                    int tupleIndex = tPointers[ptr * POINTER_LENGTH + 1];

                    hashtableFrameAccessor.reset(contents[frameIndex]);
                    outputTupleBuilder.reset();

                    for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                        outputTupleBuilder.addField(hashtableFrameAccessor, tupleIndex, k);
                    }

                    aggregatorToFlush.outputFinalResult(outputTupleBuilder, hashtableFrameAccessor, tupleIndex,
                            aggState);

                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                                outputAppender.getTupleCount());
                        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);

                        profileOutFrames++;
                        profileOutRecords += outputAppender.getTupleCount();

                        FrameUtils.flushFrame(outputBuffer, writer);
                        profileIOOutDisk++;

                        if (flushOption == GrouperFlushOption.FLUSH_FOR_GROUP_STATE) {
                            debugOptionalIODumped++;
                        } else {
                            debugOptionalIOStreamed++;
                        }
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException(
                                    "Failed to dump a group from the hash table to a frame: possibly the size of the tuple is too large.");
                        }
                    }
                }

            }
        } else {
            // directly flush the hash table contents
            for (int i = 0; i <= currentWorkingFrame && i < contents.length; i++) {
                hashtableFrameAccessor.reset(contents[i]);
                int tupleCount = hashtableFrameAccessor.getTupleCount();
                for (int j = 0; j < tupleCount; j++) {
                    outputTupleBuilder.reset();

                    for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                        outputTupleBuilder.addField(hashtableFrameAccessor, j, k);
                    }

                    aggregatorToFlush.outputFinalResult(outputTupleBuilder, hashtableFrameAccessor, j, aggState);

                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                                outputAppender.getTupleCount());
                        this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
                        profileOutFrames++;
                        profileOutRecords += outputAppender.getTupleCount();
                        FrameUtils.flushFrame(outputBuffer, writer);
                        profileIOOutNetwork++;
                        if (flushOption == GrouperFlushOption.FLUSH_FOR_GROUP_STATE) {
                            debugOptionalIODumped++;
                        } else {
                            debugOptionalIOStreamed++;
                        }
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException(
                                    "Failed to dump a group from the hash table to a frame: possibly the size of the tuple is too large.");
                        }
                    }
                }
            }
        }
        if (outputAppender.getTupleCount() > 0) {
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RECORD_OUTPUT,
                    outputAppender.getTupleCount());
            this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.FRAME_OUTPUT, 1);
            profileOutRecords += outputAppender.getTupleCount();
            FrameUtils.flushFrame(outputBuffer, writer);
            if (isGenerateRuns && this.runReaders.size() > 0) {
                profileIOOutDisk++;
            } else {
                profileIOOutNetwork++;
            }
            if (flushOption == GrouperFlushOption.FLUSH_FOR_GROUP_STATE) {
                debugOptionalIODumped++;
            } else {
                debugOptionalIOStreamed++;
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#reset()
     */
    @Override
    public void reset() throws HyracksDataException {

        super.reset();

        this.groupsInHashtable = 0;

        // reset the hash table content frame
        this.currentWorkingFrame = 0;

        // reset the lookup reference
        this.lookupFrameIndex = -1;
        this.lookupTupleIndex = -1;

        // reset header pages
        resetHeaders();
    }

    private void resetHeaders() {
        for (int i = 0; i < headers.length; i++) {
            headers[i].position(0);
            while (headers[i].position() < frameSize) {
                headers[i].putInt(-1);
            }
        }
    }

    public void fail() throws HyracksDataException {

    }

    @Override
    public void wrapup() throws HyracksDataException {
        isAllInputProcessed = true;
        if (groupsInHashtable > 0) {
            debugOptionalMaxHashtableFillRatio = Math.max(debugOptionalMaxHashtableFillRatio,
                    debugTempGroupsInHashtable / debugTempUsedSlots);
            debugTempGroupsInHashtable = 0;
            debugTempUsedSlots = 0;

            if (isGenerateRuns && this.runReaders.size() > 0) {
                IFrameWriter dumpWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(HashGrouper.class
                        .getSimpleName()), ctx.getIOManager());
                dumpWriter.open();
                flush(dumpWriter, GrouperFlushOption.FLUSH_FOR_GROUP_STATE);
                RunFileReader runReader = ((RunFileWriter) dumpWriter).createReader();
                this.runReaders.add(runReader);
                dumpWriter.close();
                this.debugCounters.updateOptionalCommonCounter(OptionalCommonCounters.RUN_GENERATED, 1);
            } else {
                flush(outputWriter, GrouperFlushOption.FLUSH_FOR_RESULT_STATE);
            }
            reset();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#close()
     */
    @Override
    public void close() throws HyracksDataException {

        this.dumpAndCleanDebugCounters();

        this.headers = null;
        this.contents = null;
    }

    public int getProcessedTupleCount() {
        return this.processedTuple;
    }

    public int getCurrentWorkingFrameIndex() {
        return this.currentWorkingFrame;
    }

    @Override
    public List<RunFileReader> getOutputRunReaders() throws HyracksDataException {
        return this.runReaders;
    }

    protected void dumpAndCleanDebugCounters() {
        this.debugCounters.updateRequiredCounter(RequiredCounters.CPU, debugRequiredCPU);
        this.debugCounters.updateRequiredCounter(RequiredCounters.IO_OUT_DISK, debugOptionalIODumped);

        this.debugCounters.updateRequiredCounter(RequiredCounters.IN_FRAMES, profileInFrames);
        this.debugCounters.updateRequiredCounter(RequiredCounters.IN_RECOEDS, profileInRecords);
        this.debugCounters.updateRequiredCounter(RequiredCounters.OUT_FRAMES, profileOutFrames);
        this.debugCounters.updateRequiredCounter(RequiredCounters.OUT_RECORDS, profileOutRecords);

        this.debugCounters.updateOptionalCustomizedCounter(".io.streamed", debugOptionalIOStreamed);
        this.debugCounters.updateOptionalCustomizedCounter(".io.dumped", debugOptionalIODumped);

        this.debugCounters.updateOptionalCustomizedCounter(".hash.groupsInTable", debugTempGroupsInHashtable);
        this.debugCounters.updateOptionalCustomizedCounter(".hash.slotsUsed", debugTempUsedSlots);

        debugOptionalMaxHashtableFillRatio = Math.max(debugOptionalMaxHashtableFillRatio,
                (double) debugTempGroupsInHashtable / debugTempUsedSlots);

        this.debugCounters.updateOptionalCustomizedCounter(".hash.maxFillRatio",
                ((long) (debugOptionalMaxHashtableFillRatio * 100)));

        this.debugCounters.updateOptionalSortCounter(OptionalSortCounters.CPU_COMPARE, debugOptionalSortCPUCompare);
        this.debugCounters.updateOptionalSortCounter(OptionalSortCounters.CPU_COPY, debugOptionalSortCPUCopy);

        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.CPU_COMPARE_HIT, debugOptionalCPUCompareHit);
        this.debugCounters
                .updateOptionalHashCounter(OptionalHashCounters.CPU_COMPARE_MISS, debugOptionalCPUCompareMiss);
        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.CPU_COMPARE, debugOptionalCPUCompareHit
                + debugOptionalCPUCompareMiss);
        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.HITS, debugOptionalHashHits);
        this.debugCounters.updateOptionalHashCounter(OptionalHashCounters.MISSES, debugOptionalHashMisses);

        this.debugCounters.dumpCounters(ctx.getCounterContext());

        this.debugRequiredCPU = 0;
        this.debugOptionalIODumped = 0;
        this.debugOptionalIOStreamed = 0;
        this.debugOptionalSortCPUCompare = 0;
        this.debugOptionalSortCPUCopy = 0;
        this.debugOptionalHashHits = 0;
        this.debugOptionalHashMisses = 0;
        this.debugOptionalCPUCompareHit = 0;
        this.debugOptionalCPUCompareMiss = 0;
        this.debugOptionalMaxHashtableFillRatio = 0;
        this.debugTempCPUCounter = 0;
        this.debugTempGroupsInHashtable = 0;
        this.debugTempUsedSlots = 0;
        this.profileInRecords = 0;
        this.profileInFrames = 0;
        this.profileOutRecords = 0;
        this.profileOutFrames = 0;
        this.debugCounters.reset();

        ctx.getCounterContext().getCounter("profile.cpu." + this.debugCounters.getDebugID(), true).update(profileCPU);
        ctx.getCounterContext().getCounter("profile.io.in.disk." + this.debugCounters.getDebugID(), true)
                .update(profileIOInDisk);
        ctx.getCounterContext().getCounter("profile.io.in.network." + this.debugCounters.getDebugID(), true)
                .update(profileIOInNetwork);
        ctx.getCounterContext().getCounter("profile.io.out.disk." + this.debugCounters.getDebugID(), true)
                .update(profileIOOutDisk);
        ctx.getCounterContext().getCounter("profile.io.out.network." + this.debugCounters.getDebugID(), true)
                .update(profileIOOutNetwork);

        profileCPU = 0;
        profileIOInDisk = 0;
        profileIOInNetwork = 0;
        profileIOOutDisk = 0;
        profileIOOutNetwork = 0;

    }

    @Override
    public List<Long> getOutputRunSizeInRows() throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getRecordsCompletelyAggregated() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getGroupsCompletelyAggregated() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public List<Long> getOutputGroupsInRows() throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }
}
