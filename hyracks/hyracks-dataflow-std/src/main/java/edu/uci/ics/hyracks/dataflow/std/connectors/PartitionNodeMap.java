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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.util.BitSet;

public class PartitionNodeMap implements Cloneable {

    final int fromNodes, toNodes;

    final BitSet nodeMap;

    public enum RedistributionStrategy {
        ONE_TO_ONE,
        FULL_HASH,
        GROUP_HASH,
        EMPTY;
    }

    RedistributionStrategy redistributionStrategy;

    public PartitionNodeMap(int fromNodes, int toNodes) {
        this.fromNodes = fromNodes;
        this.toNodes = toNodes;
        this.nodeMap = new BitSet(fromNodes * toNodes);
        this.nodeMap.clear(0, fromNodes * toNodes);
        this.redistributionStrategy = RedistributionStrategy.EMPTY;
    }

    public PartitionNodeMap(int fromNodes, int toNodes, BitSet nodeMap, RedistributionStrategy redistStrategy) {
        this.fromNodes = fromNodes;
        this.toNodes = toNodes;
        this.nodeMap = nodeMap;
        this.redistributionStrategy = redistStrategy;
    }

    public int getFromNodes() {
        return fromNodes;
    }

    public Object clone() {
        return new PartitionNodeMap(fromNodes, toNodes, (BitSet) nodeMap.clone(), this.redistributionStrategy);
    }

    public int getToNodes() {
        return toNodes;
    }

    public int[] getFromNodesFanOut() {
        int[] fanOuts = new int[fromNodes];
        for (int i = 0; i < fromNodes; i++) {
            for (int j = 0; j < toNodes; j++) {
                if (hasMap(i, j)) {
                    fanOuts[i]++;
                }
            }
        }
        return fanOuts;
    }

    public int[] getToNodesFanIn() {
        int[] fanIns = new int[toNodes];
        for (int i = 0; i < toNodes; i++) {
            for (int j = 0; j < fromNodes; j++) {
                if (hasMap(j, i)) {
                    fanIns[i]++;
                }
            }
        }
        return fanIns;
    }

    public void reset() {
        this.nodeMap.clear(0, fromNodes * toNodes);
    }

    public void setAsOneToOneNodeMap() {
        for (int i = 0; i < fromNodes; i++) {
            this.nodeMap.set(i * toNodes + i % toNodes);
        }
        this.redistributionStrategy = RedistributionStrategy.ONE_TO_ONE;
    }

    public void setAsFullyPartitionNodeMap() {
        this.nodeMap.set(0, fromNodes * toNodes);
        this.redistributionStrategy = RedistributionStrategy.FULL_HASH;
    }

    public void setMap(int fromIndex, int toIndex) {
        this.nodeMap.set(fromIndex * toNodes + toIndex);
    }

    public boolean hasMap(int fromIndex, int toIndex) {
        return this.nodeMap.get(fromIndex * toNodes + toIndex);
    }

    public boolean isFullyPartitionNodeMap() {
        for (int i = 0; i < fromNodes * toNodes; i++) {
            if (!nodeMap.get(i)) {
                return false;
            }
        }
        return true;
    }

    public boolean isOneToOnePartitionNodeMap() {
        if (fromNodes != toNodes) {
            return false;
        }
        if (nodeMap.cardinality() != toNodes) {
            return false;
        }
        for (int i = 0; i < fromNodes; i++) {
            if (!nodeMap.get(i * toNodes + i)) {
                return false;
            }
        }
        return true;
    }

    public BitSet getNodeMap() {
        return nodeMap;
    }

    public RedistributionStrategy getRedistributionStrategy() {
        return this.redistributionStrategy;
    }

    public String toString() {
        StringBuilder sbder = new StringBuilder();

        sbder.append("{\"fromNodes\":").append(fromNodes).append(",\"toNodes\":").append(toNodes)
                .append(",\"nodeMap\":[");

        boolean first = true;
        for (int i = 0; i < fromNodes * toNodes; i++) {
            if (nodeMap.get(i)) {
                if (!first) {
                    sbder.append(';');
                }
                sbder.append(i);
                first = false;
            }
        }
        sbder.append("]}");
        return sbder.toString();
    }

    public String getStarMap() {
        StringBuilder sbder = new StringBuilder();

        for (int i = 0; i < fromNodes; i++) {
            for (int j = 0; j < toNodes; j++) {
                if (nodeMap.get(i * toNodes + j)) {
                    sbder.append('*');
                } else {
                    sbder.append('o');
                }
            }
            if (i < fromNodes - 1) {
                sbder.append("\n");
            }
        }

        return sbder.toString();
    }
}
