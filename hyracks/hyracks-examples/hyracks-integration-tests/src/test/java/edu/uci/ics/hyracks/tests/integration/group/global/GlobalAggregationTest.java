/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.tests.integration.group.global;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.FloatSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MinMaxStringFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.global.DynamicHybridHashGrouper.PartSpillStrategy;
import edu.uci.ics.hyracks.dataflow.std.group.global.aggregators.AvgFieldAggregateAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.aggregators.AvgFieldFinalMergeAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.aggregators.AvgFieldPartialMergeAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.HashFunctionFamilyFactoryAdapter;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.SimpleUniformDataPartitionDescriptor;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class GlobalAggregationTest extends AbstractIntegrationTest {

    final IFileSplitProvider splitProvider = new ConstantFileSplitProvider(
            new FileSplit[] {
                    new FileSplit(NC1_ID, new FileReference(new File(
                            "/Volumes/Home/Datasets/tpch/tpch0.1/lineitem.tbl.part1"))),
                    new FileSplit(NC2_ID, new FileReference(new File(
                            "/Volumes/Home/Datasets/tpch/tpch0.1/lineitem.tbl.part2"))) });

    final RecordDescriptor desc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

    final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
            UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
            IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
            FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, }, '|');

    LocalGroupOperatorDescriptor.GroupAlgorithms localGrouper = LocalGroupOperatorDescriptor.GroupAlgorithms.SIMPLE_HYBRID_HASH;
    LocalGroupOperatorDescriptor.GroupAlgorithms globalGrouper = LocalGroupOperatorDescriptor.GroupAlgorithms.RECURSIVE_HYBRID_HASH;

    private final SimpleUniformDataPartitionDescriptor dataPartitionDesc = new SimpleUniformDataPartitionDescriptor(
            600571, new long[] { 1500, 2807 }, 1, new int[] { 0 });

    private final boolean pinLastResPart = true;

    private final PartSpillStrategy partSpillStrategy = PartSpillStrategy.MIN_ABSORB_FIRST;

    private final boolean enableResidentPart = false;

    private final int[] singleKeys = new int[] { 0 };
    private final int[] doubleKeys = new int[] { 8, 0 };
    private final int[] decorFields = new int[] {};
    private final int framesLimit = 64;
    private final int tableSize = 8171;
    private final int groupStateInBytes = 64;
    private final double fudgeFactor = 1.4;
    private final boolean useBloomfilter = true;
    private final int minFramesPerResidentPart = 2;

    private AbstractSingleActivityOperatorDescriptor getPrinter(
            JobSpecification spec,
            String prefix) throws IOException {

        ResultSetId rsId = new ResultSetId(1);
        AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                new ConstantFileSplitProvider(new FileSplit[] {
                        new FileSplit(NC1_ID, new FileReference(new File("/Volumes/Home/hyracks_tmp/201405/" + prefix
                                + "_nc1.log"))),
                        new FileSplit(NC2_ID, new FileReference(new File("/Volumes/Home/hyracks_tmp/201405/" + prefix
                                + "_nc2.log"))) }), "|");
        spec.addResultSetId(rsId);

        return printer;
    }

    /**
     * <pre>
     * select count(*), sum(L_PARTKEY), sum(L_LINENUMBER), sum(L_EXTENDEDPRICE) 
     * from LINEITEM;
     * </pre>
     * 
     * which should return
     * 
     * <pre>
     * 6005, 615388, 17990, 152774398.38
     * </pre>
     * 
     * @throws Exception
     */
    @Test
    public void noKeySumGlobalGroupTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC1_ID, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] {};

        LocalGroupOperatorDescriptor grouper0 = new LocalGroupOperatorDescriptor(spec, keyFields, new int[] {},
                dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor, new IBinaryComparatorFactory[] {},
                new IBinaryHashFunctionFamily[] {}, null, new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false),
                                new IntSumFieldAggregatorFactory(1, false), new IntSumFieldAggregatorFactory(3, false),
                                new FloatSumFieldAggregatorFactory(5, false),
                                new AvgFieldAggregateAggregatorFactory(1, false),
                                new MinMaxStringFieldAggregatorFactory(15, true, true),
                                new MinMaxStringFieldAggregatorFactory(15, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec,
                localGrouper, 0, useBloomfilter, enableResidentPart, minFramesPerResidentPart, pinLastResPart,
                partSpillStrategy);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper0, NC1_ID, NC2_ID);

        IConnectorDescriptor conn0 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn0, csvScanner, 0, grouper0, 0);

        LocalGroupOperatorDescriptor grouper1 = new LocalGroupOperatorDescriptor(spec, keyFields, new int[] {},
                dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor, new IBinaryComparatorFactory[] {},
                new IBinaryHashFunctionFamily[] {}, null, new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] {
                                new IntSumFieldAggregatorFactory(keyFields.length, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                                new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                                new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldFinalMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec,
                globalGrouper, 1, useBloomfilter, enableResidentPart, minFramesPerResidentPart, pinLastResPart,
                partSpillStrategy);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper1, NC1_ID, NC2_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {}));

        spec.connect(conn1, grouper0, 0, grouper1, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "global_" + localGrouper.name() + "_"
                + globalGrouper.name() + "_nokey");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper1, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void singleKeySumGlobalGroupTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC1_ID, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 1 };
        int[] storedKeyFields = new int[] { 0 };
        int groupCount = 20000;

        LocalGroupOperatorDescriptor grouper0 = new LocalGroupOperatorDescriptor(spec, keyFields, new int[] {},
                dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, null,
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new CountFieldAggregatorFactory(false), new IntSumFieldAggregatorFactory(1, false),
                        new IntSumFieldAggregatorFactory(3, false), new FloatSumFieldAggregatorFactory(5, false),
                        new AvgFieldAggregateAggregatorFactory(1, false),
                        new MinMaxStringFieldAggregatorFactory(15, true, true),
                        new MinMaxStringFieldAggregatorFactory(15, false, true) }), new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] {
                                new IntSumFieldAggregatorFactory(keyFields.length, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                                new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                                new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec,
                localGrouper, 0, useBloomfilter, enableResidentPart, minFramesPerResidentPart, pinLastResPart,
                partSpillStrategy);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper0, NC1_ID, NC2_ID);

        IConnectorDescriptor conn0 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn0, csvScanner, 0, grouper0, 0);

        LocalGroupOperatorDescriptor grouper1 = new LocalGroupOperatorDescriptor(spec, storedKeyFields, new int[] {},
                dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }, null,
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldFinalMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec,
                globalGrouper, 1, useBloomfilter, enableResidentPart, minFramesPerResidentPart, pinLastResPart,
                partSpillStrategy);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper1, NC1_ID, NC2_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
                spec,
                new FieldHashPartitionComputerFactory(
                        storedKeyFields,
                        new IBinaryHashFunctionFactory[] { HashFunctionFamilyFactoryAdapter
                                .getFunctionFactoryFromFunctionFamily(MurmurHash3BinaryHashFunctionFamily.INSTANCE, 1) }));

        spec.connect(conn1, grouper0, 0, grouper1, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "global_" + localGrouper.name() + "_"
                + globalGrouper.name() + "_singlekey");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper1, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void multiKeySumGlobalGroupTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC1_ID, NC2_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 8, 1 };
        int[] storedKeyFields = new int[] { 0, 1 };
        int groupCount = 59975;

        LocalGroupOperatorDescriptor grouper0 = new LocalGroupOperatorDescriptor(spec, keyFields, new int[] {},
                dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor, new IBinaryComparatorFactory[] {
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE,
                        MurmurHash3BinaryHashFunctionFamily.INSTANCE }, null, new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false),
                                new IntSumFieldAggregatorFactory(1, false), new IntSumFieldAggregatorFactory(3, false),
                                new FloatSumFieldAggregatorFactory(5, false),
                                new AvgFieldAggregateAggregatorFactory(1, false),
                                new MinMaxStringFieldAggregatorFactory(15, true, true),
                                new MinMaxStringFieldAggregatorFactory(15, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec,
                localGrouper, 0, useBloomfilter, enableResidentPart, minFramesPerResidentPart, pinLastResPart,
                partSpillStrategy);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper0, NC1_ID, NC2_ID);

        IConnectorDescriptor conn0 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn0, csvScanner, 0, grouper0, 0);

        LocalGroupOperatorDescriptor grouper1 = new LocalGroupOperatorDescriptor(spec, storedKeyFields, new int[] {},
                dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor, new IBinaryComparatorFactory[] {
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE,
                        MurmurHash3BinaryHashFunctionFamily.INSTANCE }, null, new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] {
                                new IntSumFieldAggregatorFactory(keyFields.length, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                                new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                                new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                                new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                                new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldPartialMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 2, false),
                        new FloatSumFieldAggregatorFactory(keyFields.length + 3, false),
                        new AvgFieldFinalMergeAggregatorFactory(keyFields.length + 4, false),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 5, true, true),
                        new MinMaxStringFieldAggregatorFactory(keyFields.length + 6, false, true) }), outputRec,
                globalGrouper, 1, useBloomfilter, enableResidentPart, minFramesPerResidentPart, pinLastResPart,
                partSpillStrategy);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper1, NC1_ID, NC2_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(storedKeyFields, new IBinaryHashFunctionFactory[] {
                        HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                                MurmurHash3BinaryHashFunctionFamily.INSTANCE, 1),
                        HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                                MurmurHash3BinaryHashFunctionFamily.INSTANCE, 1) }));

        spec.connect(conn1, grouper0, 0, grouper1, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "global_" + localGrouper.name() + "_"
                + globalGrouper.name() + "_multikey");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper1, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}
