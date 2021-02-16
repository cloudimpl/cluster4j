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

import java.lang.invoke.VarHandle;
import jdk.incubator.foreign.MemoryAddress;

/**
 *
 * @author nuwan
 */
public class VarHandlerEx implements MemHandler{

    private final VarHandle varHandle;
    private final MemoryAddress address;
    public VarHandlerEx(VarHandle varHandle,MemoryAddress address) {
        this.varHandle = varHandle;
        this.address = address;
    }
    
    @Override
    public void set(byte v) {
        varHandle.set(address,v);
    }

    @Override
    public void set(short v) {
        varHandle.set(address,v);
    }

    @Override
    public void set(int v) {
        varHandle.set(address,v);
    }

    @Override
    public void set(long v) {
        varHandle.set(address,v);
    }

    @Override
    public void set(double v) {
        varHandle.set(address,v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, byte v) {
        varHandle.set(address,nodeIdx,itemIdx,v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, short v) {
        varHandle.set(address,nodeIdx,itemIdx,v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, int v) {
       varHandle.set(address,nodeIdx,itemIdx,v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, long v) {
        varHandle.set(address,nodeIdx,itemIdx,v);
    }

    @Override
    public void set(long nodeIdx, long itemIdx, double v) {
       varHandle.set(address,nodeIdx,itemIdx,v);
    }

    @Override
    public byte getByte(long nodeIdx, long itemIdx) {
        return (byte)varHandle.get(address,nodeIdx,itemIdx);
    }

    @Override
    public short getShort(long nodeIdx, long itemIdx) {
        return (short)varHandle.get(address,nodeIdx,itemIdx);
    }

    @Override
    public int getInt(long nodeIdx, long itemIdx) {
       return (int)varHandle.get(address,nodeIdx,itemIdx);
    }

    @Override
    public long getLong(long nodeIdx, long itemIdx) {
        return (long)varHandle.get(address,nodeIdx,itemIdx);
    }

    @Override
    public double getDouble(long nodeIdx, long itemIdx) {
       return (double)varHandle.get(address,nodeIdx,itemIdx);
    }

    @Override
    public byte getByte() {
         return (byte)varHandle.get(address);
    }

    @Override
    public short getShort() {
        return (short)varHandle.get(address);
    }

    @Override
    public int getInt() {
        return (int)varHandle.get(address);
    }

    @Override
    public long getLong() {
        return (long)varHandle.get(address);
    }

    @Override
    public double getDouble() {
        return (double)varHandle.get(address);
    }
    
}
