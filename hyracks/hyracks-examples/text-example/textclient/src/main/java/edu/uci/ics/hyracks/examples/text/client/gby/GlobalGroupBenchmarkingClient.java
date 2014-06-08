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
package edu.uci.ics.hyracks.examples.text.client.gby;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
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

/**
 * The application client for the performance tests of the external hash group
 * operator.
 */
public class GlobalGroupBenchmarkingClient {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

        @Option(name = "-infile-splits",
                usage = "Comma separated list of file-splits for the input. A file-split is <node-name>:<path>",
                required = true)
        public String inFileSplits;

        @Option(name = "-outfile-splits", usage = "Comma separated list of file-splits for the output", required = true)
        public String outFileSplits;

        @Option(name = "-frames-limit", usage = "Number of frames (default: 1024)", required = false)
        public int framesLimit = 1024;

        @Option(name = "-algo", usage = "The algorithm to be used", required = true)
        public int algo;

        @Option(name = "-fudge-factor", usage = "Hash groupby fudge factor (default: 1.4)", required = false)
        public double fudgeFactor = 1.4;

        @Option(name = "-use-bloomfilter",
                usage = "Whether to use bloom-filter in hash table lookup for hybrid-hash algorithms (default: false)",
                required = false)
        public boolean useBloomfilter = false;

        @Option(
                name = "-enable-resident-part",
                usage = "Whether the resident partition should be further partitioned for hybrid-hash algorithm (default: false)",
                required = false)
        public boolean enableResidentPart = false;

        @Option(name = "-spill-strategy",
                usage = "The spilling strategy used for resident partitions in hyrid-hash algorithms (default: false)",
                required = false)
        public int spillStrategy = 2;

        @Option(name = "-ip-mask", usage = "The IP address mask to be applied for the key field", required = true)
        public int ipMask;

        @Option(name = "-input-count", usage = "The number of input records", required = true)
        public long inputCount;

        @Option(name = "-group-count", usage = "The estimate on the number of unique groups", required = true)
        public long groupCount;

        @Option(name = "-run-times", usage = "The number of runs for benchmarking", required = false)
        public int runTimes = 3;

        @Option(name = "-pin-last-res", usage = "Pin the last resident partition", required = false)
        public boolean pinLastResPart = false;

    }

    private static final int[] keyFields = new int[] { 0 };
    private static final int[] decorFields = new int[] {};
    private static final int groupStateInBytes = 37;
    private static final int frameSize = 32768;

    private static final int MIN_FRAMES_PER_RES_PART = 16;
    private static final int MAX_RES_PARTS = 50;

    private static final RecordDescriptor inDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

    private static final RecordDescriptor outDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE });

    private static final IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY) };
    private static final IBinaryHashFunctionFamily[] hashFactories = new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE };

    /**
     * @param args
     */
    public static void main(
            String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksConnection(options.host, options.port);

        URLClassLoader classLoader = (URLClassLoader) GlobalGroupBenchmarkingClient.class.getClassLoader();
        List<String> jars = new ArrayList<String>();
        URL[] urls = classLoader.getURLs();
        for (URL url : urls) {
            if (url.toString().endsWith(".jar")) {
                jars.add(new File(url.getPath()).getAbsolutePath());
            }
        }

        DeploymentId deployID = hcc.deployBinary(jars);

        JobSpecification job;

        for (int i = 0; i < options.runTimes; i++) {
            long start = System.currentTimeMillis();
            job = createJob(
                    parseFileSplits(options.inFileSplits),
                    parseFileSplits(options.outFileSplits, i),
                    frameSize,
                    options.framesLimit,
                    options.ipMask,
                    options.inputCount,
                    groupStateInBytes,
                    options.groupCount,
                    options.fudgeFactor
                            * ((double) groupStateInBytes
                                    + (options.useBloomfilter ? LocalGroupOperatorDescriptor.HT_MINI_BLOOM_FILTER_SIZE
                                            : 0) + LocalGroupOperatorDescriptor.HT_FRAME_REF_SIZE + LocalGroupOperatorDescriptor.HT_TUPLE_REF_SIZE)
                            / groupStateInBytes, options.algo, options.useBloomfilter, options.enableResidentPart,
                    (options.framesLimit / MIN_FRAMES_PER_RES_PART > MAX_RES_PARTS ? options.framesLimit
                            / MAX_RES_PARTS : MIN_FRAMES_PER_RES_PART), options.pinLastResPart, options.spillStrategy);

            start = System.currentTimeMillis();
            JobId jobId = hcc.startJob(deployID, job);
            hcc.waitForCompletion(jobId);

            System.out.println("alg-" + options.algo + "\t" + (System.currentTimeMillis() - start));
        }
    }

    private static FileSplit[] parseFileSplits(
            String fileSplits) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int idx = s.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("File split " + s + " not well formed");
            }
            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1))));
        }
        return fSplits;
    }

    private static FileSplit[] parseFileSplits(
            String fileSplits,
            int count) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int idx = s.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("File split " + s + " not well formed");
            }
            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1) + "_"
                    + count)));
        }
        return fSplits;
    }

    private static JobSpecification createJob(
            FileSplit[] inSplits,
            FileSplit[] outSplits,
            int frameSize,
            int framesLimit,
            int ipMask,
            long inputCount,
            int groupSize,
            long groupCount,
            double fudgeFactor,
            int alg,
            boolean useBloomfilter,
            boolean enableResidentPart,
            int minFramesPerResidentPart,
            boolean pinLastResPart,
            int spillStrategy) throws HyracksDataException {
        JobSpecification spec = new JobSpecification(frameSize);
        IFileSplitProvider splitsProvider = new ConstantFileSplitProvider(inSplits);

        ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                IPv6MarkStringParserFactory.getInstance(ipMask), DoubleParserFactory.INSTANCE }, '|');

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitsProvider,
                tupleParserFactory, inDesc);

        createPartitionConstraint(spec, csvScanner, inSplits);

        SimpleUniformDataPartitionDescriptor dataPartitionDesc = new SimpleUniformDataPartitionDescriptor(inputCount,
                new long[] { groupCount }, 1, new int[] { 0 });

        PartSpillStrategy partSpillStrategy;

        switch (spillStrategy) {
            case 0:
                partSpillStrategy = PartSpillStrategy.MIN_FIRST;
                break;
            case 1:
                partSpillStrategy = PartSpillStrategy.MAX_FIRST;
                break;
            default:
                partSpillStrategy = PartSpillStrategy.MIN_ABSORB_FIRST;

        }

        GroupAlgorithms selectedAlg;

        switch (alg) {
            case 0:
                selectedAlg = GroupAlgorithms.SORT_GROUP;
                break;
            case 1:
                selectedAlg = GroupAlgorithms.SORT_GROUP_MERGE_GROUP;
                break;
            case 2:
                selectedAlg = GroupAlgorithms.HASH_GROUP;
                break;
            case 3:
                selectedAlg = GroupAlgorithms.HASH_GROUP_SORT_MERGE_GROUP;
                break;
            case 4:
                selectedAlg = GroupAlgorithms.DYNAMIC_HYBRID_HASH_MAP;
                break;
            case 5:
                selectedAlg = GroupAlgorithms.DYNAMIC_HYBRID_HASH_REDUCE;
                break;
            case 6:
                selectedAlg = GroupAlgorithms.SIMPLE_HYBRID_HASH;
                break;
            case 7:
                selectedAlg = GroupAlgorithms.RECURSIVE_HYBRID_HASH;
                break;
            case 8:
                selectedAlg = GroupAlgorithms.PRECLUSTER;
                break;
            default:
                selectedAlg = GroupAlgorithms.HASH_GROUP_SORT_MERGE_GROUP;
        }

        LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, keyFields, decorFields,
                dataPartitionDesc, framesLimit, groupStateInBytes, fudgeFactor, comparatorFactories, hashFactories,
                null, new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new DoubleSumFieldAggregatorFactory(1, false), new CountFieldAggregatorFactory(false) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new DoubleSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false) }),
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new DoubleSumFieldAggregatorFactory(keyFields.length, false),
                        new IntSumFieldAggregatorFactory(keyFields.length + 1, false) }), outDesc, selectedAlg, 0,
                useBloomfilter, enableResidentPart, minFramesPerResidentPart, pinLastResPart, partSpillStrategy);

        createPartitionConstraint(spec, grouper, inSplits);

        IConnectorDescriptor conn0 = new OneToOneConnectorDescriptor(spec);

        spec.connect(conn0, csvScanner, 0, grouper, 0);

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(outSplits);

        AbstractSingleActivityOperatorDescriptor writer = new PlainFileWriterOperatorDescriptor(spec, outSplitProvider,
                "|");

        createPartitionConstraint(spec, writer, outSplits);

        IConnectorDescriptor groupOutConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(groupOutConn, grouper, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }

    private static void createPartitionConstraint(
            JobSpecification spec,
            IOperatorDescriptor op,
            FileSplit[] splits) {
        String[] parts = new String[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            parts[i] = splits[i].getNodeName();
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, parts);
    }
}