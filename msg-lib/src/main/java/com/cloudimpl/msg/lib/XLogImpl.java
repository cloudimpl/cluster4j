/*
 * Copyright 2021 nuwan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.msg.lib;

import com.cloudimpl.mem.lib.MemoryManager;
import com.cloudimpl.mem.lib.OffHeapMemory;
import com.cloudimpl.mem.lib.OffHeapMemoryManager;
import com.cloudimpl.mem.lib.UnsafeMemoryManager;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 *
 * @author nuwan
 */
public class XLogImpl implements XLog {

    public static final byte RS = 30;
    private final MemoryManager memMan;
    private final String logPath;
    private XLogSegment current;
    private int currentSegmentId;
    private final int blockSize;
    private long pos;
    private final StringBuilder pathBuilder = new StringBuilder(0);

    public XLogImpl(String logName, String logPath, int blockSize, MemoryManager memMan) {
        this.blockSize = blockSize;
        this.memMan = memMan;
        this.logPath = logPath;
        this.currentSegmentId = 0;
        this.pos = 0;
        this.current = nextSegment(currentSegmentId);
    }

    private XLogSegment nextSegment(int segmentId) {
        pathBuilder.append(logPath).append("/").append(segmentId).append(".xlog");
        OffHeapMemory mem = memMan.mapFromPath(Path.of(pathBuilder), 0, this.blockSize, FileChannel.MapMode.READ_WRITE);
        pathBuilder.setLength(0);
        return new XLogSegment(mem, blockSize);
    }

    @Override
    public long append(CharSequence record) {
        long temp = pos;
        append(0, record);
        append(RS);
        this.pos += record.length() + 1;
        return temp;
    }

    private void append(int offset, CharSequence record) {
        if (current.isFull()) {
            current = nextSegment(++currentSegmentId);
        }
        int min = Math.min(current.remaining(), record.length() - offset);
        current.append(record, offset, min);
        if (record.length() - (offset + min) > 0) {
            append(offset + min, record);
        }

    }

    private void append(byte record) {
        if (current.isFull()) {
            current = nextSegment(++currentSegmentId);
        }
        current.append(record);
    }

    public static void main(String[] args) {
        XLog log = new XLogImpl("test", "/Users/nuwan/data", 1024 * 1024, new UnsafeMemoryManager());
        int i = 0;
        while (i < 1000000) {
            log.append("test"+i);
            log.append("test"+i);
            log.append("test"+i);
            i++;
        }

    }
}
