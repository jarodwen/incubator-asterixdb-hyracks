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
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * A state-less frame tuple appender implementation. Its implementation assumes:<br/>
 * - The frame size can be obtained by {@link ByteBuffer#capacity()};<br/>
 * - The frame structure is the typical hyracks frame structure (tuples, tuple end offsets, and tuple count)
 * <p/>
 */
public class StateLessFrameTupleAppender {

    public static final int INT_SIZE = 4;

    public static boolean append(ByteBuffer buffer, int[] fieldSlots, byte[] bytes, int offset, int length)
            throws HyracksDataException {

        int frameSize = buffer.capacity();

        int tupleCount = buffer.getInt(frameSize - INT_SIZE);

        int tupleDataEndOffset = (tupleCount == 0) ? 0 : buffer.getInt(frameSize - INT_SIZE * (tupleCount + 1));

        if (tupleDataEndOffset + fieldSlots.length * 4 + length + 4 + (tupleCount + 1) * 4 <= frameSize) {
            for (int i = 0; i < fieldSlots.length; ++i) {
                buffer.putInt(tupleDataEndOffset + i * 4, fieldSlots[i]);
            }
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset + fieldSlots.length * 4, length);
            tupleDataEndOffset += fieldSlots.length * 4 + length;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public static void reset(ByteBuffer buffer) {
        buffer.putInt(FrameHelper.getTupleCountOffset(buffer.capacity()), 0);
    }

    public static void removeLastTuple(ByteBuffer buffer) {
        int tupleCountOffset = FrameHelper.getTupleCountOffset(buffer.capacity());

        int tupleCount = buffer.getInt(tupleCountOffset);

        if (tupleCount > 0) {
            buffer.putInt(tupleCountOffset, tupleCount - 1);
        }
    }

    public static int getTupleCount(ByteBuffer buffer) {
        return buffer.getInt(FrameHelper.getTupleCountOffset(buffer.capacity()));
    }
}
