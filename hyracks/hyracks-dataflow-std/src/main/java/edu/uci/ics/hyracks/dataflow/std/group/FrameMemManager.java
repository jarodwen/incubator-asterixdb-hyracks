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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class FrameMemManager {

    private ByteBuffer[] frames;

    private int[] framesNext;

    private boolean[] framesAvailable;

    private int freeFrame;

    private final IHyracksTaskContext ctx;

    private final int INT_SIZE = 4;

    public static final int INVALID_FRAME_ID = -1;

    public FrameMemManager(
            int memCapacity,
            IHyracksTaskContext ctx) {
        this.frames = new ByteBuffer[memCapacity];
        this.framesNext = new int[memCapacity];
        this.freeFrame = 0;
        for (int i = 0; i < memCapacity - 1; i++) {
            this.framesNext[i] = i + 1;
        }
        this.framesAvailable = new boolean[memCapacity];
        for (int i = 0; i < memCapacity; i++) {
            this.framesAvailable[i] = true;
        }
        this.framesNext[memCapacity - 1] = INVALID_FRAME_ID;
        this.ctx = ctx;
    }

    /**
     * allocate a new frame, by returning the frame index
     * 
     * @return
     */
    public int allocateFrame() {
        int newFrameIdx = -1;
        if (freeFrame >= 0) {
            newFrameIdx = freeFrame;
            freeFrame = framesNext[freeFrame];
        }
        if (newFrameIdx >= 0) {
            framesNext[newFrameIdx] = INVALID_FRAME_ID;
        }

        // reset the frame content, if any
        if (newFrameIdx >= 0 && frames[newFrameIdx] != null) {
            frames[newFrameIdx].clear();
            frames[newFrameIdx].putInt(frames[newFrameIdx].array().length - INT_SIZE, 0);
        }
        if (newFrameIdx >= 0) {
            framesAvailable[newFrameIdx] = false;
            framesNext[newFrameIdx] = -1;
        }
        return newFrameIdx;
    }

    /**
     * allocate several frames together, by chaining them together.
     * 
     * @param framesCount
     * @return
     */
    public int bulkAllocate(
            int framesCount) {
        int prevFrameIdx = allocateFrame();
        framesNext[prevFrameIdx] = INVALID_FRAME_ID;
        int currentFrameIdx = prevFrameIdx;
        for (int i = 1; i < framesCount; i++) {
            currentFrameIdx = allocateFrame();
            if (currentFrameIdx < 0) {
                // failed to allocate enough pages: recycle all allocated pages
                while (prevFrameIdx >= 0) {
                    recycleFrame(prevFrameIdx);
                    prevFrameIdx = framesNext[prevFrameIdx];
                }

            } else {
                framesNext[currentFrameIdx] = prevFrameIdx;
                prevFrameIdx = currentFrameIdx;
            }
        }
        return currentFrameIdx;
    }

    /**
     * get the next frame chained with the given frame.
     * 
     * @param frameId
     * @return
     */
    public int getNextFrame(
            int frameIdx) {

        if (frameIdx < 0 || frameIdx > frames.length - 1) {
            return INVALID_FRAME_ID;
        }

        return framesNext[frameIdx];
    }

    /**
     * set the next frame pointer.
     * 
     * @param frameIdx
     * @param nextFrameIdx
     */
    public void setNextFrame(
            int frameIdx,
            int nextFrameIdx) {
        framesNext[frameIdx] = nextFrameIdx;
    }

    public boolean isFrameInitialized(
            int frameIdx) {
        return frames[frameIdx] != null;
    }

    /**
     * Get the byte buffer for the given frame index.
     * 
     * @param frameIdx
     * @return
     * @throws HyracksDataException
     */
    public ByteBuffer getFrame(
            int frameIdx) throws HyracksDataException {
        if (frameIdx < 0 || frameIdx > frames.length - 1) {
            return null;
        }

        if (frames[frameIdx] == null) {
            frames[frameIdx] = ctx.allocateFrame();
        }
        return frames[frameIdx];
    }

    public void recycleFrame(
            int frameIdx) {
        if (frameIdx < 0 || frameIdx > frames.length - 1) {
            // if the index is out of range, do nothing
            return;
        }

        if (framesAvailable[frameIdx]) {
            return;
        }

        // do the actual recycle, by inserting the page into the beginning of the free frame list
        framesNext[frameIdx] = freeFrame;
        freeFrame = frameIdx;

        framesAvailable[frameIdx] = true;
        resetFrame(frameIdx);
    }

    public void resetFrame(
            int frameIdx) {
        // reset the frame content, if any
        if (frames[frameIdx] != null) {
            frames[frameIdx].clear();
            frames[frameIdx].putInt(frames[frameIdx].array().length - INT_SIZE, 0);
        }
    }

    public void close() {
        for (int i = 0; i < frames.length; i++) {
            frames[i] = null;
        }
        framesNext = null;
        framesAvailable = null;
    }

}
