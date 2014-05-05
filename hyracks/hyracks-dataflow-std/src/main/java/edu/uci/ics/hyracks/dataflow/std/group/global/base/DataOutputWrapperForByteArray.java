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

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.data.std.api.IValueReference;

public class DataOutputWrapperForByteArray implements DataOutput {

    private IValueReference valRef;

    public DataOutputWrapperForByteArray() {
        valRef = null;
    }

    public void reset(IValueReference valRef) {
        this.valRef = valRef;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#write(int)
     */
    @Override
    public void write(int b) throws IOException {
        if (valRef != null) {

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#write(byte[])
     */
    @Override
    public void write(byte[] b) throws IOException {
        if (valRef != null) {
            if (b.length <= valRef.getLength()) {

            }
        }
        throw new IOException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#write(byte[], int, int)
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (valRef != null) {
            if (len <= valRef.getLength()) {

            }
        }
        throw new IOException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeBoolean(boolean)
     */
    @Override
    public void writeBoolean(boolean v) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeByte(int)
     */
    @Override
    public void writeByte(int v) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeShort(int)
     */
    @Override
    public void writeShort(int v) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeChar(int)
     */
    @Override
    public void writeChar(int v) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeInt(int)
     */
    @Override
    public void writeInt(int v) throws IOException {
        if (valRef != null) {
            if (valRef.getLength() >= 4) {
                
            }
        }
        throw new IOException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeLong(long)
     */
    @Override
    public void writeLong(long v) throws IOException {
        if (valRef != null) {
            if (valRef.getLength() >= 8) {
                
            }
        }
        throw new IOException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeFloat(float)
     */
    @Override
    public void writeFloat(float v) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeDouble(double)
     */
    @Override
    public void writeDouble(double v) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeBytes(java.lang.String)
     */
    @Override
    public void writeBytes(String s) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeChars(java.lang.String)
     */
    @Override
    public void writeChars(String s) throws IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataOutput#writeUTF(java.lang.String)
     */
    @Override
    public void writeUTF(String s) throws IOException {
        // TODO Auto-generated method stub

    }

}
