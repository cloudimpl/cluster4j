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
import java.nio.ByteOrder;
import jdk.incubator.foreign.GroupLayout;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.SequenceLayout;
import org.agrona.UnsafeAccess;

/**
 *
 * @author nuwan
 */
public class Test {

    public static void main(String[] args) {
        int count = 30_000_000;
        GroupLayout layout = MemoryLayout.ofStruct(MemoryLayout.ofSequence(count, MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder())).withName("keySequence"),
                MemoryLayout.ofSequence(count, MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder())).withName("valueSequence"));

        //flat sequence
        MemorySegment mem = MemorySegment.allocateNative(layout);
        VarHandle keyHandler = layout.varHandle(long.class, MemoryLayout.PathElement.groupElement("keySequence"), MemoryLayout.PathElement.sequenceElement());
        VarHandle valueHandler = layout.varHandle(long.class, MemoryLayout.PathElement.groupElement("valueSequence"), MemoryLayout.PathElement.sequenceElement());
        MemoryAddress addr = mem.baseAddress();
        UnsafeAccess.UNSAFE.getAndAddInt(null, mem.baseAddress().toRawLongValue(), count);
        System.out.println("sequence layout "+UnsafeAccess.UNSAFE.getAndAddInt(null, mem.baseAddress().toRawLongValue(), count));
        int j = 0;
        while (j < 10) {
            long start = System.currentTimeMillis();
            long i = 0;
            while (i < count) {

                keyHandler.set(addr, i, i);
                valueHandler.set(addr, i, i * 1000);
                i++;
            }
            long end = System.currentTimeMillis();
            System.out.println("ops:"+((double)(end - start) * 1000)/count+" total time:"+(end -start));
            j++;
        }
        mem.close();
        int itemCount = 512;
        int nodeCount = (int) Math.ceil(((double)count/itemCount));
        int shift = Math.getExponent(itemCount);
        layout = MemoryLayout.ofStruct(MemoryLayout.ofSequence(nodeCount,MemoryLayout.ofSequence(itemCount, MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()))).withName("keySequence"),
                MemoryLayout.ofSequence(nodeCount, MemoryLayout.ofSequence(itemCount,MemoryLayout.ofValueBits(Integer.SIZE, ByteOrder.nativeOrder()))).withName("valueSequence"));
        mem = MemorySegment.allocateNative(layout);
        keyHandler = layout.varHandle(long.class, MemoryLayout.PathElement.groupElement("keySequence"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        valueHandler = layout.varHandle(int.class, MemoryLayout.PathElement.groupElement("valueSequence"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
         addr = mem.baseAddress();
        System.out.println("node partition layout");
        j = 0;
        while (j < 10) {
            long start = System.currentTimeMillis();
            long i = 0;
            while (i < count) {
                long nodeIdx = i >> shift;
                long itemIdx = i & (itemCount - 1);
                keyHandler.set(addr,nodeIdx, itemIdx, i);
                valueHandler.set(addr,nodeIdx,itemIdx, (int)(i * 1000));
                i++;
            }
            long end = System.currentTimeMillis();
            System.out.println("ops:"+((double)(end - start) * 1000)/count+" total time:"+(end -start));
            j++;
        }
        
        mem.close();
        layout = MemoryLayout.ofStruct(MemoryLayout.ofSequence(nodeCount,MemoryLayout.ofSequence(itemCount, MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()))).withName("keySequence"),
                MemoryLayout.ofSequence(nodeCount, MemoryLayout.ofSequence(itemCount,MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()))).withName("valueSequence"));
        
        nodeCount = (int) Math.ceil(((double)count/(itemCount/2)));
        SequenceLayout leafLayout = MemoryLayout.ofSequence(nodeCount, MemoryLayout.ofSequence(itemCount, MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()))).withName("leafLayout");
        mem = MemorySegment.allocateNative(leafLayout);
        VarHandle leafHandler = leafLayout.varHandle(long.class, MemoryLayout.PathElement.sequenceElement(),MemoryLayout.PathElement.sequenceElement());
       // valueHandler = layout.varHandle(long.class, MemoryLayout.PathElement.groupElement("valueSequence"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        addr = mem.baseAddress();
        System.out.println("leaf partition layout");
        j = 0;
        shift = 8;
        itemCount = itemCount/2;
        while (j < 10) {
            long start = System.currentTimeMillis();
            long i = 0;
            while (i < count) {
                long nodeIdx = i >> shift;
                long itemIdx = i & (itemCount - 1);
                leafHandler.set(addr,nodeIdx, itemIdx, i);
                leafHandler.set(addr,nodeIdx,itemIdx + 256, i * 1000);
            //    System.out.println("i:"+i + "nodeIdx:"+nodeIdx + " itemIdx:"+itemIdx);
                i++;
            }
            long end = System.currentTimeMillis();
            System.out.println("ops:"+((double)(end - start) * 1000)/count+" total time:"+(end -start));
            j++;
        }
    }
}
