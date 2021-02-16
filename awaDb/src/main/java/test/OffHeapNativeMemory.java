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
import java.nio.ByteBuffer;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;

/**
 *
 * @author nuwan
 */
public class OffHeapNativeMemory implements OffHeapMemory{

    private final MemorySegment memSegment;
    private final MemoryAddress addr;

    public OffHeapNativeMemory(MemorySegment memSegment,MemoryAddress addr) {
        this.memSegment = memSegment;
        this.addr = addr;
    }
    
    @Override
    public long addr() {
        return this.addr.toRawLongValue();
    }
    
    @Override
    public MemHandler memHandler(Class<?> type,MemoryLayout mainLayout,String targetLayout,MemoryLayout.PathElement... elements)
    {
        MemoryLayout.PathElement el = elements[0];
        VarHandle varHandler = mainLayout.varHandle(type, elements);
        return new VarHandlerEx(varHandler, addr);
    }

    @Override
    public void close() {
        memSegment.close();
    }

    @Override
    public long size() {
        return this.memSegment.byteSize();
    }

    @Override
    public ByteBuffer asByteBuffer() {
        return this.memSegment.asByteBuffer();
    }
}
