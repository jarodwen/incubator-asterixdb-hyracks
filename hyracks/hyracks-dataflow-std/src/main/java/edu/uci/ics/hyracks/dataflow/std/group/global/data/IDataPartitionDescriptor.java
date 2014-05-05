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

/**
 * The interface to describe the statistics of a dataset. 
 */
public interface IDataPartitionDescriptor {

    /**
     * Get the number of rows in the dataset.
     * 
     * @return
     */
    public long getRawRowCount();

    /**
     * Get the number of unique rows (i.e., no duplicates) in the dataset.
     * 
     * @return
     */
    public long getUniqueRowCount();

    /**
     * Get the number of unique keys for the given key columns.
     * 
     * @param columnIDs the key columns
     * @return
     */
    public long getUniqueRowCount(int[] columnIDs);

    /**
     * Get the number of rows in the given partition.
     * 
     * @param partID the partition id
     * @return
     */
    public long getRawRowCountInPartition(int partID);

    /**
     * Get the number of unique rows (i.e., no duplicates) in the given partition of the dataset.
     * 
     * @param partID the partition id
     * @return
     */
    public long getUniqueRowCountInPartition(int partID);

    /**
     * Get the number of unique keys for the given key columns, in the specific partitions
     * 
     * @param columnIDs the key columns
     * @param partID the partition id
     * @return
     */
    public long getUniqueRowCountInPartition(int[] columnIDs, int partID);

}
