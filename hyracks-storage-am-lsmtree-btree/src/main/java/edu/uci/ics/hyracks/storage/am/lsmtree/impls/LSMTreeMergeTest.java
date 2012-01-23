package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import java.io.DataOutput;
import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public class LSMTreeMergeTest extends AbstractLSMTreeTest {
    private static final int PAGE_SIZE = 256;
    private static final int NUM_PAGES = 30;
    private static final int MAX_OPEN_FILES = 100;
    private static final int HYRACKS_FRAME_SIZE = 128;
    private IHyracksTaskContext ctx = TestUtils.create(HYRACKS_FRAME_SIZE);

    @Test
    public void Test1() throws Exception {
        System.out.printf("TEST1 START\n");
        // in disk
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        IBufferCache bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx);
        IFileMapProvider fmp = TestStorageManagerComponentHolder.getFileMapProvider(ctx);
        FileReference file = new FileReference(new File(fileName));
        bufferCache.createFile(file);
        int fileId = fmp.lookupFileId(file);
        bufferCache.openFile(fileId);

        // in memory
        InMemoryBufferCacheFactory InMemBufferCacheFactory = new InMemoryBufferCacheFactory(PAGE_SIZE, NUM_PAGES);
        IBufferCache memBufferCache = InMemBufferCacheFactory.createInMemoryBufferCache();

        // declare fields
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparator[] cmps = new IBinaryComparator[keyFieldCount];
        cmps[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();

        MultiComparator cmp = new MultiComparator(cmps);

        LSMTypeAwareTupleWriterFactory insertTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
        LSMTypeAwareTupleWriterFactory deleteTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

        ITreeIndexFrameFactory insertLeafFrameFactory = new BTreeNSMLeafFrameFactory(insertTupleWriterFactory);
        ITreeIndexFrameFactory deleteLeafFrameFactory = new BTreeNSMLeafFrameFactory(deleteTupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        IBTreeLeafFrame insertLeafFrame = (IBTreeLeafFrame) insertLeafFrameFactory.createFrame();

        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, fileId, 0, metaFrameFactory);
        IFreePageManager memFreePageManager = new InMemoryFreePageManager(NUM_PAGES, metaFrameFactory);

        // For the Flush Mechanism
        LSMEntireTupleWriterFactory flushTupleWriterFactory = new LSMEntireTupleWriterFactory(typeTraits);
        ITreeIndexFrameFactory flushLeafFrameFactory = new BTreeNSMLeafFrameFactory(flushTupleWriterFactory);
        FreePageManagerFactory freePageManagerFactory = new FreePageManagerFactory(bufferCache, metaFrameFactory);
        BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, fieldCount,
                interiorFrameFactory, flushLeafFrameFactory);

        LSMTree lsmtree = new LSMTree(memBufferCache, bufferCache, fieldCount, cmp, memFreePageManager,
                interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory, bTreeFactory,
                (IFileMapManager) fmp);

        // LSMTree lsmtree = LSMTreeUtils.createLSMTree(10, 30, 2,
        // memBufferCache, bufferCache, fileId, typeTraits,
        // cmp.getComparators(), BTreeLeafFrameType.REGULAR_NSM,
        // (IFileMapManager)fmp);

        lsmtree.create(fileId);
        lsmtree.open(fileId);

        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        DataOutput dos = tb.getDataOutput();

        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);

        IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        accessor.reset(frame);

        FrameTupleReference tuple = new FrameTupleReference();

        int resultSize = 100000;
        int[][] resultArray = new int[resultSize][2];

        // insert 0~250 tuples
        System.out.printf("Start for 1st Insert\n");
        ITreeIndexAccessor lsmTreeAccessor = lsmtree.createAccessor();
        for (int i = 0; i < 251; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i;
        }
        for (int i = 0; i < 251; i++) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            ArrayTupleReference t = new ArrayTupleReference();
            t.reset(tb.getFieldEndOffsets(), tb.getByteArray());

            try {
                lsmTreeAccessor.insert(t);
            } catch (TreeIndexException e) {
                System.out.println("test03:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.printf("Start for 2nd Insert\n");
        // delete 126~250.
        for (int i = 126; i < 251; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i;
        }
        for (int i = 126; i < 251; i++) {
            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            ArrayTupleReference t = new ArrayTupleReference();
            t.reset(tb.getFieldEndOffsets(), tb.getByteArray());

            try {
                lsmTreeAccessor.delete(t);
            } catch (TreeIndexException e) {
                System.out.println("test03:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // insert 251~1
        for (int i = 251; i > 0; i--) {
            resultArray[i][0] = i;
            resultArray[i][1] = i;
        }
        for (int i = 251; i > 0; i--) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            ArrayTupleReference t = new ArrayTupleReference();
            t.reset(tb.getFieldEndOffsets(), tb.getByteArray());

            try {
                lsmTreeAccessor.insert(t);
            } catch (TreeIndexException e) {
                System.out.println("test03:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // delete 100~0
        for (int i = 100; i >= 0; i--) {
            resultArray[i][0] = i;
            resultArray[i][1] = i;
        }
        for (int i = 100; i >= 0; i--) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            ArrayTupleReference t = new ArrayTupleReference();
            t.reset(tb.getFieldEndOffsets(), tb.getByteArray());

            try {
                lsmTreeAccessor.delete(t);
            } catch (TreeIndexException e) {
                System.out.println("test03:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // insert 1~50
        for (int i = 1; i < 51; i++) {
            resultArray[i][0] = i;
            resultArray[i][1] = i;
        }
        for (int i = 1; i < 51; i++) {

            int f0 = resultArray[i][0];
            int f1 = resultArray[i][1];

            tb.reset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f0, dos);
            tb.addFieldEndOffset();
            IntegerSerializerDeserializer.INSTANCE.serialize(f1, dos);
            tb.addFieldEndOffset();

            appender.reset(frame, true);
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());

            tuple.reset(accessor, 0);

            ArrayTupleReference t = new ArrayTupleReference();
            t.reset(tb.getFieldEndOffsets(), tb.getByteArray());

            try {
                lsmTreeAccessor.insert(t);
            } catch (TreeIndexException e) {
                System.out.println("test03:" + e);
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        lsmtree.merge();

        // Output should be:
        // In memory tree = 0->del, 1~50 ins
        // MergedTree = 0->ins, 1~100->del, 101~251->ins
        // Whole search = 1~50,101~251

        // System.out.println("LSMTreeFlushTest: start print the first tree");
        // lsmtree.scanDiskTree(1);
        //
        // Print out the first in Disk Btree
        // System.out.println("LSMTreeFlushTest: start print the first tree");
        // lsmtree.scanDiskTree(0);
        // Print out the In-memory Tree
        System.out.println("LSMTreeFlushTest: start print the In-memory tree");
        lsmtree.scanInMemoryTree();
        // TODO: scan whole tree
        /*
         * System.out.println("Range SEARCH:");
         * 
         * BTreeOpContext searchOpCtx = lsmtree.createOpContext(IndexOp.SEARCH);
         * ITreeIndexCursor rangeCursor = new LSMTreeRangeSearchCursor();
         * 
         * // build low and high keys ArrayTupleBuilder ktb = new
         * ArrayTupleBuilder(cmp.getKeyFieldCount()); DataOutput kdos =
         * ktb.getDataOutput();
         * 
         * ISerializerDeserializer[] keyDescSers = {
         * IntegerSerializerDeserializer.INSTANCE }; RecordDescriptor keyDesc =
         * new RecordDescriptor(keyDescSers); IFrameTupleAccessor keyAccessor =
         * new FrameTupleAccessor( ctx.getFrameSize(), keyDesc);
         * keyAccessor.reset(frame);
         * 
         * appender.reset(frame, true);
         * 
         * // build and append low key ktb.reset();
         * IntegerSerializerDeserializer.INSTANCE.serialize(-1, kdos);
         * ktb.addFieldEndOffset(); appender.append(ktb.getFieldEndOffsets(),
         * ktb.getByteArray(), 0, ktb.getSize());
         * 
         * // build and append high key ktb.reset();
         * IntegerSerializerDeserializer.INSTANCE.serialize(300, kdos);
         * ktb.addFieldEndOffset(); appender.append(ktb.getFieldEndOffsets(),
         * ktb.getByteArray(), 0, ktb.getSize());
         * 
         * // create tuplereferences for search keys FrameTupleReference lowKey
         * = new FrameTupleReference(); lowKey.reset(keyAccessor, 0);
         * 
         * FrameTupleReference highKey = new FrameTupleReference();
         * highKey.reset(keyAccessor, 1);
         * 
         * IBinaryComparator[] searchCmps = new IBinaryComparator[1];
         * searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE
         * .createBinaryComparator(); MultiComparator searchCmp = new
         * MultiComparator(searchCmps);
         * 
         * RangePredicate rangePred = new RangePredicate(true, lowKey, highKey,
         * true, true, searchCmp, searchCmp); lsmtree.search(rangeCursor,
         * rangePred, searchOpCtx);
         * 
         * try { while (rangeCursor.hasNext()) { rangeCursor.next();
         * ITupleReference frameTuple = rangeCursor.getTuple(); String rec =
         * TupleUtils.printTuple(frameTuple, recDescSers); if
         * (((LSMTypeAwareTupleReference) frameTuple).isDelete()) {
         * System.out.println("del " + rec); } else { System.out.println("ins "
         * + rec); } // System.out.println("------------------"); } } catch
         * (Exception e) { e.printStackTrace(); } finally { rangeCursor.close();
         * }
         */
        lsmtree.close();
        bufferCache.closeFile(fileId);
        memBufferCache.close();

        System.out.printf("End of TEST1()\n");

    }
}