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
package com.cloudimpl.mem.lib;

import io.questdb.std.Unsafe;

/**
 *
 * @author nuwan
 */
public class UnsafeVarHandler implements MemHandler{
    private long addr;
    private long[] sizes;

    public UnsafeVarHandler(long addr, long[] sizes) {
        this.addr = addr;
        this.sizes = sizes;
    }
    
    @Override
    public void set(byte v) {
         Unsafe.getUnsafe().putByte(addr, v);
    }

    @Override
    public void set(short v) {
        Unsafe.getUnsafe().putShort(addr, v);
    }

    @Override
    public void set(int v) {
        Unsafe.getUnsafe().putInt(addr, v);
    }

    @Override
    public void set(long v) {
        Unsafe.getUnsafe().putLong(addr, v);
    }

    @Override
    public void set(double v) {
        Unsafe.getUnsafe().putDouble(addr, v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, byte v) {
        Unsafe.getUnsafe().putByte(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx), v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, short v) {
       Unsafe.getUnsafe().putShort(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx), v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, int v) {
        Unsafe.getUnsafe().putInt(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx), v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, long v) {
        Unsafe.getUnsafe().putLong(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx), v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, double v) {
        Unsafe.getUnsafe().putDouble(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx), v);
    }

    @Override
    public byte getByte(long nodeIdx, long itemIdx) {
        return Unsafe.getUnsafe().getByte(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx));
    }

    @Override
    public short getShort(long nodeIdx, long itemIdx) {
        return Unsafe.getUnsafe().getShort(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx));
    }

    @Override
    public int getInt(long nodeIdx, long itemIdx) {
        return Unsafe.getUnsafe().getInt(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx));
    }

    @Override
    public long getLong(long nodeIdx, long itemIdx) {
        return Unsafe.getUnsafe().getLong(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx));
    }

    @Override
    public double getDouble(long nodeIdx, long itemIdx) {
        return Unsafe.getUnsafe().getDouble(addr + (sizes[0] * nodeIdx) + (sizes[1] * itemIdx));
    }

    @Override
    public byte getByte() {
        return Unsafe.getUnsafe().getByte(addr);
    }

    @Override
    public short getShort() {
        return Unsafe.getUnsafe().getShort(addr);
    }

    @Override
    public int getInt() {
        return Unsafe.getUnsafe().getInt(addr);
    }

    @Override
    public long getLong() {
        return Unsafe.getUnsafe().getLong(addr);
    }

    @Override
    public double getDouble() {
        return Unsafe.getUnsafe().getDouble(addr);
    }

    @Override
    public void set(long offset, byte v) {
          Unsafe.getUnsafe().putByte(addr + offset,v);
    }

    @Override
    public void set(long offset, short v) {
          Unsafe.getUnsafe().putShort(addr + offset,v);
    }

    @Override
    public void set(long offset, int v) {
        Unsafe.getUnsafe().putInt(addr + offset,v);
    }

    @Override
    public void set(long offset, long v) {
        Unsafe.getUnsafe().putLong(addr + offset,v);
    }

    @Override
    public void set(long offset, double v) {
         Unsafe.getUnsafe().putDouble(addr + offset,v);
    }

    @Override
    public byte getByte(long offset) {
        return Unsafe.getUnsafe().getByte(addr + offset);
    }

    @Override
    public short getShort(long offset) {
        return Unsafe.getUnsafe().getShort(addr + offset);
    }

    @Override
    public int getInt(long offset) {
         return Unsafe.getUnsafe().getInt(addr + offset);
    }

    @Override
    public long getLong(long offset) {
        return Unsafe.getUnsafe().getLong(addr + offset);
    }

    @Override
    public double getDouble(long offset) {
       return Unsafe.getUnsafe().getDouble(addr + offset);
    }
    
}
