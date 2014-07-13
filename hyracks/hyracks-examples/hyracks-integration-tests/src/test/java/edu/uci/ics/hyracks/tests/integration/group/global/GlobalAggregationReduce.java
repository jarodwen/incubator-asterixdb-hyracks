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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
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
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.DoubleSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.global.DynamicHybridHashGrouper.PartSpillStrategy;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor.GroupAlgorithms;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPv6MarkStringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.data.SimpleUniformDataPartitionDescriptor;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class GlobalAggregationReduce extends AbstractIntegrationTest {

    private final int[] keyFields = new int[] { 0 };
    private final int[] decorFields = new int[] {};
    private final int framesLimit = 128;
    private final int groupStateInBytes = 37;
    private final double fudgeFactor = 1.4;
    private final boolean useBloomfilter = false;
    private final int[] ipMasks = new int[] { 0, 1, 2, 3, 5 };
    private final long[] groupCounts = new long[] { 10000000, 62500000, 3607602 };// { 10000000, 9283319, 3607602,
                                                                                  // 244144, 4096
    // };
    private final int inputDataOption = 0;
    private final boolean enableResidentPart = true;
    private final int minFramesPerResidentPart = 3;

    private final boolean pinLastResPart = false;

    private final PartSpillStrategy partSpillStrategy = PartSpillStrategy.MAX_FIRST;

    final IFileSplitProvider splitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(
                    NC1_ID,
                    new FileReference(
                            new File(
                             "/Volumes/Home/Datasets/AggBench/v20130119/origin/z05_1000000000_10000000.dat"))) });
                            // "/Volumes/Home/Datasets/AggBench/v20130119/origin/z0_1000000000_1000000000_sorted.dat.shuffled.dat")))
                            // });
                            //        "/Volumes/Home/Datasets/AggBench/v20130119/small/z0_1000000000_1000000000_sorted.dat.shuffled.dat.small"))) });

    final RecordDescriptor inDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

    final RecordDescriptor outDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE });

    final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
            IPv6MarkStringParserFactory.getInstance(ipMasks[inputDataOption], true), DoubleParserFactory.INSTANCE },
            '|');

    private final SimpleUniformDataPartitionDescriptor dataPartitionDesc = new SimpleUniformDataPartitionDescriptor(
            1000000000, new long[] { groupCounts[inputDataOption] }, 1, new int[] { 0 });

    final IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY) };
    final IBinaryHashFunctionFamily[] hashFactories = new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE };

    // LocalGroupOperatorDescriptor.GroupAlgorithms grouperAlgo =
    // LocalGroupOperatorDescriptor.GroupAlgorithms.DYNAMIC_HYBRID_HASH_REDUCE;

    private AbstractSingleActivityOperatorDescriptor getPrinter(
            JobSpecification spec,
            String prefix) throws IOException {

        ResultSetId rsId = new ResultSetId(1);
        AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                        "/Volumes/Home/hyracks_tmp/201405/" + prefix + ".log"))) }), "|");
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
    public void globalMapTest() throws Exception {

        for (boolean enableResPart : new boolean[] { true, false })
            for (boolean pinLastResidentPart : new boolean[] { true }) {
                for (PartSpillStrategy spillStrategy : new PartSpillStrategy[] { PartSpillStrategy.LOWER_ABSORB_THAN_AVG }) {
                    for (GroupAlgorithms grouperAlgo : new GroupAlgorithms[] { GroupAlgorithms.SIMPLE_HYBRID_HASH }) {

                        JobSpecification spec = new JobSpecification();

                        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider,
                                tupleParserFactory, inDesc);

                        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC1_ID);

                        LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, keyFields,
                                decorFields, dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor,
                                comparatorFactories, hashFactories, null, new MultiFieldsAggregatorFactory(
                                        new IFieldAggregateDescriptorFactory[] {
                                                new DoubleSumFieldAggregatorFactory(1, false),
                                                new CountFieldAggregatorFactory(false) }),
                                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                                        new DoubleSumFieldAggregatorFactory(keyFields.length, false),
                                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false) }),
                                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                                        new DoubleSumFieldAggregatorFactory(keyFields.length, false),
                                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false) }), outDesc,
                                grouperAlgo, 0, useBloomfilter, enableResPart, minFramesPerResidentPart,
                                pinLastResidentPart, spillStrategy);

                        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC1_ID);

                        IConnectorDescriptor conn0 = new OneToOneConnectorDescriptor(spec);

                        spec.connect(conn0, csvScanner, 0, grouper, 0);

                        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
                                "reducer_" + grouperAlgo.name() + (pinLastResidentPart ? "_pl_" : "")
                                        + (enableResPart ? "_erp_" : "") + spillStrategy.name());

                        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

                        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
                        spec.connect(conn2, grouper, 0, printer, 0);

                        spec.addRoot(printer);
                        runTest(spec);

                    }

                }
            }
    }
}
