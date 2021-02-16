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
package test;

import io.questdb.std.Unsafe;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import jdk.incubator.foreign.GroupLayout;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.SequenceLayout;
import jdk.incubator.foreign.ValueLayout;

/**
 *
 * @author nuwan
 */
public class UnsafeNativeMemory implements OffHeapMemory {

    private final long addr;
    private final long size;

    public UnsafeNativeMemory(long addr,long size) {
        this.size = size;
        this.addr = addr;
    }

    @Override
    public long addr() {
        return addr;
    }

    @Override
    public MemHandler memHandler(Class<?> type, MemoryLayout mainLayout, String targetLayoutName, MemoryLayout.PathElement... elements) {
        int len = elements.length - 1;
        long[] sizes;
        if (len > 0) {
            sizes = new long[len];
        } else {
            sizes = new long[0];
        }
        long offset = mainLayout.byteOffset(elements[0]);
        MemoryLayout targetLayout = findLayout(mainLayout, targetLayoutName);
        if (targetLayout instanceof ValueLayout) {
            return new UnsafeVarHandler(addr, sizes);
        } else {
            SequenceLayout seq = (SequenceLayout) targetLayout;
            fillSizes(0, sizes, seq.elementLayout());
            return new UnsafeVarHandler(addr + offset, sizes);
        }

    }

    @Override
    public void close() {
        Unsafe.getUnsafe().freeMemory(addr);
    }

    @Override
    public long size() {
        return this.size;
    }

    private MemoryLayout findLayout(MemoryLayout rootLayout, String name) {
        if (rootLayout instanceof GroupLayout) {
            GroupLayout group = (GroupLayout) rootLayout;
            return group.memberLayouts().stream().filter(layout -> layout.name().isPresent()).filter(layout -> layout.name().get().equals(name)).findFirst().orElseThrow();
        } else {
            throw new RuntimeException("rootLayout not a group layout");
        }
    }

    private void fillSizes(int index, long[] sizes, MemoryLayout layout) {
        if (layout instanceof SequenceLayout) {
            SequenceLayout seqLayout = (SequenceLayout) layout;
            sizes[index] = seqLayout.byteSize();
            fillSizes(index + 1, sizes, seqLayout.elementLayout());
        } else if (layout instanceof ValueLayout) {
            ValueLayout valueLayout = (ValueLayout) layout;
            sizes[index] = valueLayout.byteSize();
        } else {
            throw new RuntimeException("invalid layout" + layout);
        }
    }

    @Override
    public ByteBuffer asByteBuffer() {
        try {
            Field address = Buffer.class.getDeclaredField("address");
            address.setAccessible(true);
            Field capacity = Buffer.class.getDeclaredField("capacity");
            capacity.setAccessible(true);

            ByteBuffer bb = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());
            address.setLong(bb, addr);
            capacity.setInt(bb, (int) size);
            bb.clear();
            return bb;
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }
}
