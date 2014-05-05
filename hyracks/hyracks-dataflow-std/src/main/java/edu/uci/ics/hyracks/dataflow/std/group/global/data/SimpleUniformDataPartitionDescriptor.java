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
package edu.uci.ics.hyracks.dataflow.std.group.global.data;

import java.io.Serializable;

public class SimpleUniformDataPartitionDescriptor implements IDataPartitionDescriptor, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The total number of raw records in the dataset.
     */
    private final long rawRecordsCount;

    /**
     * The unique key counts for all columns, assuming a uniform distribution.
     */
    private final long[] uniqueKeyCountForColumns;

    /**
     * The number of partitions for the dataset.
     */
    private final int numParts;

    /**
     * The primary key columns for the dataset (which are used to distinguish the records)
     */
    private final int[] primaryKeyColumns;

    public SimpleUniformDataPartitionDescriptor(long rawRecordsCount, long[] uniqueKeys, int numPartitions,
            int[] primaryKeys) {
        this.rawRecordsCount = rawRecordsCount;
        this.uniqueKeyCountForColumns = uniqueKeys;
        this.numParts = numPartitions;
        this.primaryKeyColumns = primaryKeys;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.data.IDataPartitionDescriptor#getRawRowCount()
     */
    @Override
    public long getRawRowCount() {
        return rawRecordsCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.data.IDataPartitionDescriptor#getUniqueRowCount()
     */
    @Override
    public long getUniqueRowCount() {
        return getUniqueRowCount(primaryKeyColumns);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.data.IDataPartitionDescriptor#getUniqueRowCount(int[])
     */
    @Override
    public long getUniqueRowCount(int[] columnIDs) {
        // FIXME Right now we assume the first column unique key count is the primary key count
        return uniqueKeyCountForColumns[0];
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.data.IDataPartitionDescriptor#getRawRowCountInPartition(int)
     */
    @Override
    public long getRawRowCountInPartition(int partID) {
        return getRawRowCount() / numParts;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.global.data.IDataPartitionDescriptor#getUniqueRowCountInPartition(int)
     */
    @Override
    public long getUniqueRowCountInPartition(int partID) {
        return getUniqueRowCount() / numParts;
    }

    /**
     * Return the number of unique rows for the given columns for the given partition, assuming a random
     * selection from the original dataset.
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.data.IDataPartitionDescriptor#getUniqueRowCountInPartition(int[],
     *      int)
     */
    @Override
    public long getUniqueRowCountInPartition(int[] columnIDs, int partID) {
        return getUniqueRowCount(columnIDs) / numParts;
    }

}
