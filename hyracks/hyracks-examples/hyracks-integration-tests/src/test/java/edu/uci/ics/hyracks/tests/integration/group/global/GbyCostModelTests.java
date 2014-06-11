/*
 * Copyright 2009-2014 by The Regents of the University of California
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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

public class GbyCostModelTests {

    @Test
    public void test() {
        double R = 1000000000;
        double r = 37;
        double B = 32768;
        for (double G : new double[] { 1000000000, 441000000, 62500000, 244144 }) {
            for (double m : new double[] { 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16382, 32768, 65536,
                    131072 }) {
                CostVector cv = new CostVector();
                long[] ca = sortGroupMergeGroupCostComputer(cv, R, G, r, m, B, true);
                System.out.println(cv.toSimpleString());
            }
        }
    }

    public static long[] sortGroupMergeGroupCostComputer(
            CostVector costVector,
            double rawRecordsCount,
            double uniqueKeyCount,
            double recordSizeInBytes,
            double memoryInPages,
            double pageSizeInBytes,
            boolean hasMergeGroup) {

        String operatorPrefix = "sortGroup";
        if (hasMergeGroup) {
            operatorPrefix += "MergeGroup";
        }

        double debugSortCPUCost = 0;
        double dblTemp = 0;

        costVector.updateKeyValue(operatorPrefix + ".inputRecords", rawRecordsCount);
        costVector.updateKeyValue(operatorPrefix + ".inputKeys", uniqueKeyCount);
        costVector.updateKeyValue(operatorPrefix + ".inputFrames", rawRecordsCount * recordSizeInBytes
                / pageSizeInBytes);
        costVector.updateIoInNetwork(rawRecordsCount * recordSizeInBytes / pageSizeInBytes);

        double recordsInFullMemory = (memoryInPages - 1) * pageSizeInBytes / recordSizeInBytes;
        double keysInFullMemory = getKeysInRecords(recordsInFullMemory, rawRecordsCount, uniqueKeyCount);
        int sortedRunsCount = (int) (Math.ceil(rawRecordsCount / recordsInFullMemory));
        if (sortedRunsCount <= 1) {
            // all records can be sorted in memory
            dblTemp = sortCPUCostComputer(rawRecordsCount, uniqueKeyCount);
            debugSortCPUCost += dblTemp;
            costVector.updateCpu(dblTemp);
            costVector.updateKeyValue(operatorPrefix + ".outputRecords", uniqueKeyCount);
            costVector.updateKeyValue(operatorPrefix + ".outputFrames", uniqueKeyCount * recordSizeInBytes
                    / pageSizeInBytes);
            if (hasMergeGroup) {
                costVector.updateIoOutDisk(uniqueKeyCount * recordSizeInBytes / pageSizeInBytes);
            } else {
                costVector.updateIoOutNetwork(uniqueKeyCount * recordSizeInBytes / pageSizeInBytes);
            }
            return new long[] { (long) Math.ceil(uniqueKeyCount), (long) Math.ceil(uniqueKeyCount) };
        }
        List<Double> sortedRuns = new ArrayList<Double>();
        List<Double> rawRecordsRepresentedBySortedRuns = new ArrayList<Double>();

        // for the number of records to be outputted
        double outputRecordCount = 0;

        // for the first (sortedRunsCount - 1) parts:
        for (int i = 0; i < sortedRunsCount - 1; i++) {
            sortedRuns.add(keysInFullMemory);
            rawRecordsRepresentedBySortedRuns.add(recordsInFullMemory);
        }
        dblTemp = (sortedRunsCount - 1) * sortCPUCostComputer(recordsInFullMemory, keysInFullMemory);
        debugSortCPUCost += dblTemp;
        costVector.updateCpu(dblTemp);
        outputRecordCount += ((sortedRunsCount - 1) * keysInFullMemory);

        // for the last part
        double rawRecordsForLastPart = rawRecordsCount - (sortedRunsCount - 1) * recordsInFullMemory;
        double keysInLastPart = getKeysInRecords(rawRecordsForLastPart, rawRecordsCount, uniqueKeyCount);
        sortedRuns.add(keysInLastPart);
        rawRecordsRepresentedBySortedRuns.add(rawRecordsForLastPart);
        dblTemp = sortCPUCostComputer(rawRecordsForLastPart, keysInLastPart);
        debugSortCPUCost += dblTemp;
        costVector.updateCpu(dblTemp);

        outputRecordCount += keysInLastPart;

        costVector.updateKeyValue(operatorPrefix + ".cpu.sort", debugSortCPUCost);

        // do merge-group
        if (hasMergeGroup) {
            costVector.updateIoOutDisk(outputRecordCount * recordSizeInBytes / pageSizeInBytes);
            mergeGroupCostComputer(costVector, rawRecordsCount, uniqueKeyCount, recordSizeInBytes, sortedRuns,
                    rawRecordsRepresentedBySortedRuns, memoryInPages, pageSizeInBytes);
            outputRecordCount = uniqueKeyCount;
        }
        costVector.updateIoOutNetwork(outputRecordCount * recordSizeInBytes / pageSizeInBytes);

        costVector.updateKeyValue(operatorPrefix + ".outputRecords", outputRecordCount);
        costVector.updateKeyValue(operatorPrefix + ".outputFrames", outputRecordCount * recordSizeInBytes
                / pageSizeInBytes);

        if (outputRecordCount < uniqueKeyCount) {
            throw new IllegalStateException("Invalid state for SortGroup cost model");
        }

        return new long[] { (long) Math.ceil(outputRecordCount), (long) Math.ceil(uniqueKeyCount) };
    }

    /**
     * Compute the merge-group cost.
     * 
     * @param costVector
     * @param recordsInserted
     * @param keysInserted
     * @param estimatedRecordSizeInByte
     * @param sortedRuns
     * @param rawRecordsRepresentedBySortedRuns
     * @param ctx
     */
    public static void mergeGroupCostComputer(
            CostVector costVector,
            double recordsInserted,
            double keysInserted,
            double estimatedRecordSizeInByte,
            List<Double> sortedRuns,
            List<Double> rawRecordsRepresentedBySortedRuns,
            double memoryInPages,
            double pageSizeInBytes) {

        double lastOutDiskIO = 0;

        while (sortedRuns.size() > 0) {
            // do a full merge
            double rawRecordsRepresentedByMergedRun = 0.0;
            double recordsToMerge = 0.0;
            int i = 0;
            for (i = 0; i < memoryInPages && sortedRuns.size() > 0; i++) {
                rawRecordsRepresentedByMergedRun += rawRecordsRepresentedBySortedRuns.remove(0);
                recordsToMerge += sortedRuns.remove(0);
            }
            // update the merge CPU cost
            costVector.updateCpu(recordsToMerge * Math.max((Math.log(i) / Math.log(2)), 1));

            // always assume that the input of a merge-grouper is from disk; if it is from network, sorted
            // runs can be merged through hash-merge connector
            costVector.updateIoInDisk(recordsToMerge * estimatedRecordSizeInByte / pageSizeInBytes);

            // update the merge IO cost for flushing and loading the merged file

            double recordsAfterMerge = getKeysInRecords(rawRecordsRepresentedByMergedRun, recordsInserted, keysInserted);

            costVector.updateIoOutDisk(recordsAfterMerge * estimatedRecordSizeInByte / pageSizeInBytes);

            lastOutDiskIO = recordsAfterMerge * estimatedRecordSizeInByte / pageSizeInBytes;

            if (sortedRuns.size() > 0) {
                sortedRuns.add(recordsAfterMerge);
                rawRecordsRepresentedBySortedRuns.add(rawRecordsRepresentedByMergedRun);
            }
        }

        // The last round of merge will produce no out-disk I/O
        costVector.updateIoOutDisk(lastOutDiskIO * -1);
    }

    private static double sortCPUCostComputer(
            double rawRecordsCount,
            double uniqueKeysCount) {
        if (rawRecordsCount == 0 && uniqueKeysCount == 0) {
            return 0;
        }
        return Math.max(0, rawRecordsCount * (Math.log(rawRecordsCount) / Math.log(2) - 1) + 1);
    }

    public static double getKeysInRecords(
            double recordCount,
            double totalRecords,
            double totalKeys) {
        if (totalRecords == totalKeys) {
            return recordCount;
        }
        if (recordCount >= totalRecords) {
            return totalKeys;
        }
        return totalKeys * (1 - Math.pow(1 - recordCount / totalRecords, totalRecords / totalKeys));
    }

    public class CostVector {
        double cpu, ioOutDisk, ioOutNetwork, ioInNetwork, ioInDisk;

        HashMap<String, Double> keyValueParis = new HashMap<>();

        public CostVector(
                CostVector cvToCopy) {
            this.cpu = cvToCopy.cpu;
            this.ioOutDisk = cvToCopy.ioOutDisk;
            this.ioInDisk = cvToCopy.ioInDisk;
            this.ioOutNetwork = cvToCopy.ioOutNetwork;
            this.ioInNetwork = cvToCopy.ioInNetwork;
            for (String key : keyValueParis.keySet()) {
                this.keyValueParis.put(new String(key), new Double(keyValueParis.get(key)));
            }
        }

        public void updateKeyValue(
                String key,
                double value) {
            if (keyValueParis.containsKey(key)) {
                value += keyValueParis.get(key);
            }
            keyValueParis.put(key, value);
        }

        public double getCpu() {
            return cpu;
        }

        public void setCpu(
                double cpu) {
            this.cpu = cpu;
        }

        public void updateCpu(
                double cpuDelta) {
            if (Double.isInfinite(cpuDelta) || Double.isNaN(cpuDelta)) {
                throw new IllegalStateException("Invalid CPU cost value.");
            }
            this.cpu += cpuDelta;
        }

        public double getIoOutDisk() {
            return ioOutDisk;
        }

        public void setIoOutDisk(
                double io) {
            this.ioOutDisk = io;
        }

        public void updateIoOutDisk(
                double ioDelta) {
            this.ioOutDisk += ioDelta;
        }

        public double getIoOutNetwork() {
            return ioOutNetwork;
        }

        public void setIoOutNetwork(
                double network) {
            this.ioOutNetwork = network;
        }

        public void updateIoOutNetwork(
                double networkDelta) {
            this.ioOutNetwork += networkDelta;
        }

        public double getIoInNetwork() {
            return ioInNetwork;
        }

        public void setIoInNetwork(
                double ioInNetwork) {
            this.ioInNetwork = ioInNetwork;
        }

        public void updateIoInNetwork(
                double ioInNetwork) {
            this.ioInNetwork += ioInNetwork;
        }

        public double getIoInDisk() {
            return ioInDisk;
        }

        public void setIoInDisk(
                double ioInDisk) {
            this.ioInDisk = ioInDisk;
        }

        public void updateIoInDisk(
                double ioInDisk) {
            this.ioInDisk += ioInDisk;
        }

        public void updateWithCostVector(
                CostVector cv) {
            this.cpu += cv.cpu;
            this.ioOutDisk += cv.ioOutDisk;
            this.ioOutNetwork += cv.ioOutNetwork;
            this.ioInDisk += cv.ioInDisk;
            this.ioInNetwork += cv.ioInNetwork;
        }

        public void updateForParallel(
                int parallelism) {
            this.cpu *= parallelism;
            this.ioOutDisk *= parallelism;
            this.ioOutNetwork *= parallelism;
            this.ioInDisk *= parallelism;
            this.ioInNetwork *= parallelism;
        }

        public String toSimpleString() {
            return this.cpu + "\t" + this.ioOutDisk;
        }

        public String toString() {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append("{\"profile.cpu\":").append(this.cpu).append(",\"profile.io.in.disk\":")
                    .append(this.ioInDisk).append(",\"profile.io.in.network\":").append(this.ioInNetwork)
                    .append(",\"profile.io.out.disk\":").append(this.ioOutDisk).append(",\"profile.io.out.network\":")
                    .append(this.ioOutNetwork);
            if (keyValueParis != null && keyValueParis.keySet().size() > 0) {
                strBuilder.append(", \"keyValues\":{");
                boolean isFirst = true;
                for (String key : keyValueParis.keySet()) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        strBuilder.append(",");
                    }
                    strBuilder.append("\"").append(key).append("\":").append(keyValueParis.get(key));
                }
                strBuilder.append("}");
            }
            strBuilder.append("}");
            return strBuilder.toString();
        }

        public String toStringWithNodeName(
                String nodeName,
                String nodeType) {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append("{\"").append(nodeName).append(".profile.cpu\":").append(this.cpu).append(",\"")
                    .append(nodeName).append(".profile.io.in.disk\":").append(this.ioInDisk).append(",\"")
                    .append(nodeName).append(".profile.io.in.network\":").append(this.ioInNetwork).append(",\"")
                    .append(nodeName).append(".profile.io.out.disk\":").append(this.ioOutDisk).append(",\"")
                    .append(nodeName).append(".profile.io.out.network\":").append(this.ioOutNetwork);
            // strBuilder.append("{\"").append(nodeName).append(".costmodel.").append(nodeType).append(".required.cpu\":")
            // .append(this.cpu).append(",\"").append(nodeName).append(".costmodel.").append(nodeType)
            // .append(".required.io\":").append(this.ioOutDisk).append(",\"").append(nodeName).append(".costmodel.")
            // .append(nodeType).append(".required.network\":").append(this.ioOutNetwork);
            if (keyValueParis != null && keyValueParis.keySet().size() > 0) {
                strBuilder.append(", \"keyValues\":{");
                boolean isFirst = true;
                for (String key : keyValueParis.keySet()) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        strBuilder.append(",");
                    }
                    strBuilder.append("\"").append(nodeName).append(".costmodel.").append(nodeType)
                            .append(".optional.").append(key).append("\":").append(keyValueParis.get(key));
                }
                strBuilder.append("}");
            }
            strBuilder.append("}");
            return strBuilder.toString();
        }

        public CostVector() {
            this.cpu = 0;
            this.ioOutDisk = 0;
            this.ioOutNetwork = 0;
            this.ioInDisk = 0;
            this.ioInNetwork = 0;
        }
    }
}
