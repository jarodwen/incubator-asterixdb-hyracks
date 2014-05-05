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
package edu.uci.ics.hyracks.dataflow.std.group.global.data;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;

public class HashTableFrameTupleAppender {

    private static final int INT_SIZE = 4;

    private final int frameSize;
    private final int listRefSize;

    private ByteBuffer buffer;

    private int tupleCount;

    private int tupleDataEndOffset;

    public HashTableFrameTupleAppender(int frameSize, int listRefSize) {
        this.frameSize = frameSize;
        this.listRefSize = listRefSize;
    }

    public void reset(ByteBuffer buffer, boolean clear) {
        this.buffer = buffer;
        if (clear) {
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
            tupleCount = 0;
            tupleDataEndOffset = 0;
        } else {
            tupleCount = buffer.getInt(FrameHelper.getTupleCountOffset(frameSize));
            tupleDataEndOffset = tupleCount == 0 ? 0 : buffer.getInt(FrameHelper.getTupleCountOffset(frameSize)
                    - tupleCount * 4);
        }
    }

    public boolean append(int[] fieldSlots, byte[] bytes, int offset, int length, int refFrameIndex, int refTupleIndex) {
        if (tupleDataEndOffset + fieldSlots.length * 4 + length + 4 + (tupleCount + 1) * 4 + listRefSize <= frameSize) {
            for (int i = 0; i < fieldSlots.length; ++i) {
                buffer.putInt(tupleDataEndOffset + i * 4, fieldSlots[i]);
            }
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset + fieldSlots.length * 4, length);
            buffer.putInt(tupleDataEndOffset + fieldSlots.length * 4 + length, refFrameIndex);
            buffer.putInt(tupleDataEndOffset + fieldSlots.length * 4 + length + INT_SIZE, refTupleIndex);

            tupleDataEndOffset += fieldSlots.length * 4 + length + listRefSize;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }
    
    public int getTupleCount() {
        return tupleCount;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

}
