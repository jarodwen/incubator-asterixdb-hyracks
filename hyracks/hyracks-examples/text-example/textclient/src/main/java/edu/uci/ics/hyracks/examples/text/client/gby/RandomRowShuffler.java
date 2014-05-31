/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.examples.text.client.gby;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.shuffle.RandomRowShufflerInputOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.shuffle.RandomRowShufflerOutputOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;

public class RandomRowShuffler {

    public static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-frames-limit", usage = "memory budget in frames", required = true)
        public int framesLimit;

        @Option(name = "-input-files", usage = "Comma separated list of files as the input. ", required = true)
        public String inputFiles;

        @Option(name = "-output-file", usage = "Comma separated list of files as the output.", required = true)
        public String outputFiles;
    }

    private static void createPartitionConstraint(
            JobSpecification spec,
            IOperatorDescriptor op,
            FileSplit[] splits) {
        String[] partitions = new String[splits.length];
        for (int i = 0; i < splits.length; i++) {
            partitions[i] = splits[i].getNodeName();
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, partitions);
    }

    private static final Pattern COMMA_SPLITTER = Pattern.compile(",");

    private static final Pattern COLON_SPLITTER = Pattern.compile(":");

    public static JobSpecification createJob(
            Options options) throws Exception {

        JobSpecification spec = new JobSpecification();

        String[] inFiles = COMMA_SPLITTER.split(options.inputFiles);
        FileSplit[] inSplits = new FileSplit[inFiles.length];
        for (int i = 0; i < inSplits.length; i++) {
            String[] split = COLON_SPLITTER.split(inFiles[i]);
            inSplits[i] = new FileSplit(split[0], new FileReference(new File(split[1])));
        }

        FileScanOperatorDescriptor fileScanner = new FileScanOperatorDescriptor(spec, new ConstantFileSplitProvider(
                inSplits), new DelimitedDataTupleParserFactory(
                new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, '&'), new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE }));
        createPartitionConstraint(spec, fileScanner, inSplits);

        RandomRowShufflerInputOperatorDescriptor inShuffler = new RandomRowShufflerInputOperatorDescriptor(spec,
                20121127);

        IConnectorDescriptor loadShufflerConnector = new OneToOneConnectorDescriptor(spec);

        spec.connect(loadShufflerConnector, fileScanner, 0, inShuffler, 0);

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, options.framesLimit,
                new int[] { 0 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(LongPointable.FACTORY) },
                new RecordDescriptor(new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE,
                        UTF8StringSerializerDeserializer.INSTANCE }));

        IConnectorDescriptor shufflerSorterConnector = new OneToOneConnectorDescriptor(spec);

        spec.connect(shufflerSorterConnector, inShuffler, 0, sorter, 0);

        RandomRowShufflerOutputOperatorDescriptor outShuffler = new RandomRowShufflerOutputOperatorDescriptor(spec);

        IConnectorDescriptor sorterOutShuffler = new OneToOneConnectorDescriptor(spec);

        spec.connect(sorterOutShuffler, sorter, 0, outShuffler, 0);

        // construct output file splits
        String[] outFiles = COMMA_SPLITTER.split(options.outputFiles);
        FileSplit[] outSplits = new FileSplit[outFiles.length];
        for (int i = 0; i < outFiles.length; i++) {
            String[] split = COLON_SPLITTER.split(outFiles[i]);
            outSplits[i] = new FileSplit(split[0], new FileReference(new File(split[1])));
        }

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(outSplits);

        AbstractOperatorDescriptor writer = new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, "");

        createPartitionConstraint(spec, writer, outSplits);

        IConnectorDescriptor shufflerWriterConnector = new OneToOneConnectorDescriptor(spec);
        spec.connect(shufflerWriterConnector, outShuffler, 0, writer, 0);

        return spec;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(
            String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        JobSpecification spec = createJob(options);

        IHyracksClientConnection hcc = new HyracksConnection(options.host, options.port);

        URLClassLoader classLoader = (URLClassLoader) RandomRowShuffler.class.getClassLoader();
        List<String> jars = new ArrayList<String>();
        URL[] urls = classLoader.getURLs();
        for (URL url : urls) {
            if (url.toString().endsWith(".jar")) {
                System.out.println("[INFO] Deploying jar: " + url.toString());
                jars.add(new File(url.getPath()).getAbsolutePath());
            }
        }

        DeploymentId deployID = hcc.deployBinary(jars);

        JobId jobId = hcc.startJob(deployID, spec);
        hcc.waitForCompletion(jobId);
    }

}
