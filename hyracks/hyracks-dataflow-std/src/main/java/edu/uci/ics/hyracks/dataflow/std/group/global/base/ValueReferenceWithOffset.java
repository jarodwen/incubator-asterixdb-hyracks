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
package edu.uci.ics.hyracks.dataflow.std.group.global.base;

import edu.uci.ics.hyracks.data.std.api.IValueReference;

public class ValueReferenceWithOffset implements IValueReference {

    private IValueReference valRef;
    private int offset;

    public ValueReferenceWithOffset() {

    }

    public void reset(IValueReference valRef, int offset) {
        this.valRef = valRef;
        this.offset = offset;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.data.std.api.IValueReference#getByteArray()
     */
    @Override
    public byte[] getByteArray() {
        return valRef.getByteArray();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.data.std.api.IValueReference#getStartOffset()
     */
    @Override
    public int getStartOffset() {
        return valRef.getStartOffset() + offset;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.data.std.api.IValueReference#getLength()
     */
    @Override
    public int getLength() {
        return valRef.getLength() - offset;
    }

}
