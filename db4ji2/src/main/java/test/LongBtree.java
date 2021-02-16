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

import com.cloudimpl.db4ji2.core.old.Validation;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.function.Function;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;

/**
 *
 * @author nuwan
 */
public class LongBtree extends AbstractBTree {
    
    private final LongComparable comparator;

    public LongBtree(int maxItemCount, int pageSize, Function<MemoryLayout, MemorySegment> memoryProvider) {
        super(maxItemCount, pageSize, Long.BYTES, long.class,Long.BYTES, long.class,Integer.BYTES,int.class, memoryProvider);
        this.comparator = Long::compare;
    }
    
    @Override
    protected void fillIdxNode(long dstNodeIdx, long dstItemIdx, VarHandle itemHandler, long srcNodeIdx, long srcItemIdx) {
        long key = (long) itemHandler.get(address, srcNodeIdx, srcItemIdx);
        idxItemHandler.set(address, dstNodeIdx, dstItemIdx, key);
    }
    
    public void put(long key, long value) {
        if (this.currentItemIndex >= this.maxItemCount) {
            throw new BTreeException("btree is full:" + this.currentItemIndex);
        }
        long keyNodeIdx = currentItemIndex >> this.keyIdxExponent;
        long KeyitemIdx = currentItemIndex & (this.keyNodeCapacity - 1);
        
        if (this.currentItemIndex == 0) {
            this.minKeyHandler.set(this.address, key);
            setStartingOffset(value);
        }
        this.keyItemHandler.set(this.address, keyNodeIdx, KeyitemIdx, key);
        setValue(value);
        this.currentItemIndex++;
        if (this.currentItemIndex == this.maxItemCount) {
            this.maxKeyHandler.set(this.address, key);
        }
    }
    
    @Override
    public long getMaxKeyAsLong() {
        return (long) this.maxKeyHandler.get(this.address);
    }
    
    @Override
    public long getMinKeyAsLong() {
        return (long) this.minKeyHandler.get(this.address);
    }
    
    public final Iterator findEq(Iterator ite, long key) {
        int leafNodeIdx = findLeafNode(0, 0, key);
        int size = Math.min(this.currentItemIndex - (leafNodeIdx * this.keyNodeCapacity), this.keyNodeCapacity);
        int pos = binarySearch(keyItemHandler, leafNodeIdx, size, key);
        if (pos >= 0) {
            pos = adjustEqLowPos((leafNodeIdx * this.keyNodeCapacity) + pos, key);
            return ite.withEqKey(key,0).init(this, pos, getSize());
        }
        return Iterator.EMPTY;
    }
    
    public Iterator findGE(Iterator ite, long key) {
        int leafNodeIdx = findLeafNode(0, 0, key);
        int size = Math.min(this.currentItemIndex - (leafNodeIdx * this.keyNodeCapacity), this.keyNodeCapacity);
        int pos = binarySearch(keyItemHandler, leafNodeIdx * this.keyNodeCapacity, size, key);
        if (pos >= 0) {
            pos = adjustEqLowPos((leafNodeIdx * this.keyNodeCapacity) + pos, key);
            return ite.init(this, pos, getSize());
        } else {
            pos = -pos - 1;
            return ite.init(this, (leafNodeIdx * this.keyNodeCapacity) + pos, getSize());
        }
    }
    
    public Iterator findGT(Iterator ite, int key) {
        int leafNodeIdx = findLeafNode(0, 0, key);
        int size = Math.min(this.currentItemIndex - (leafNodeIdx * this.keyNodeCapacity), this.keyNodeCapacity);
        int pos = binarySearch(keyItemHandler, leafNodeIdx * this.keyNodeCapacity, size, key);
        if (pos >= 0) {
            pos = adjustEqUpperPos((leafNodeIdx * this.keyNodeCapacity) + pos, key);
            return ite.init(this, pos, getSize());
        } else {
            pos = -pos - 1;
            return ite.init(this, (leafNodeIdx * this.keyNodeCapacity) + pos, getSize());
        }
    }
    
    public Iterator findLE(Iterator ite, int key) {
        int leafNodeIdx = findLeafNode(0, 0, key);
        int size = Math.min(this.currentItemIndex - (leafNodeIdx * this.keyNodeCapacity), this.keyNodeCapacity);
        int pos = binarySearch(keyItemHandler, leafNodeIdx * this.keyNodeCapacity, size, key);
        if (pos >= 0) {
            pos = adjustEqUpperPos((leafNodeIdx * this.keyNodeCapacity) + pos, key);
            return ite.init(this, 0, pos);
        } else {
            pos = -pos - 1;
            return ite.init(this, 0, (leafNodeIdx * this.keyNodeCapacity) + pos);
        }
    }
    
    public Iterator findLT(Iterator ite, int key) {
        int leafNodeIdx = findLeafNode(0, 0, key);
        int size = Math.min(this.currentItemIndex - (leafNodeIdx * this.keyNodeCapacity), this.keyNodeCapacity);
        int pos = binarySearch(keyItemHandler, leafNodeIdx * this.keyNodeCapacity, size, key);
        if (pos >= 0) {
            pos = adjustEqLowPos((leafNodeIdx * this.keyNodeCapacity) + pos, key);
            return ite.init(this, 0, pos);
        } else {
            pos = -pos - 1;
            return ite.init(this, 0, (leafNodeIdx * this.keyNodeCapacity) + pos);
        }
    }
    
    public LongEntry getEntry(int index, LongEntry entry) {
        
        return entry.with(getKey(index), getValue(index));
    }
    
    private long getKey(VarHandle itemHandler, int nodeIdx, int itemIdx) {
        return (long) itemHandler.get(this.address, nodeIdx, itemIdx);
    }
    
    private int adjustEqLowPos(int pos, long matchKey) {
        while (pos >= 0 && getKey(pos) == matchKey) {
            pos--;
        }
        return pos + 1;
    }
    
    private int adjustEqUpperPos(int pos, long matchKey) {
        while (pos < this.currentItemIndex && getKey(pos) == matchKey) {
            pos++;
        }
        return pos;
    }
    
    public final long getKey(int index) {
        int nodeIdx = index >> this.keyIdxExponent;
        int itemIdx = index & this.keyNodeCapacity - 1;
        return (long) this.keyItemHandler.get(this.address, nodeIdx, itemIdx);
    }
    
    protected int binarySearch(VarHandle itemHandler, int nodeIdx, int size, long key) {
        int low = 0;
        int high = size - 1;
        
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = getKey(itemHandler, nodeIdx, mid);

            //if (midVal < key)
            int ret = comparator.compare(midVal, key);
            if (ret < 0) {
                low = mid + 1;
            } else if (ret > 0) //else if (midVal > key) 
            {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }
    
    private int findLeafNode(int level, int itemIdx, long key) {
        if (level == this.levelItemCount.length) {
            return itemIdx;
        }
        
        int realOffset = this.levelStartOffset[level] + (itemIdx * this.idxNodeCapacity);
        
        int size = Math.min(this.levelStartOffset[level] + this.levelItemCount[level] - realOffset, this.idxNodeCapacity);
        int pos = binarySearch(idxItemHandler, realOffset >> this.keyIdxExponent, size, key);
        if (pos >= 0) {
            return findLeafNode(level + 1, (itemIdx * this.idxNodeCapacity) + pos + 1, key);
        } else {
            pos = -pos - 1;
            return findLeafNode(level + 1, (itemIdx * this.idxNodeCapacity) + pos, key);
        }
    }
    
    @Override
    public long getKeyAsLong(int index) {
        return getKey(index);
    }
    
    @Override
    public int getMaxExp() {
        return 0;
    }
    
    public static void main(String[] args) {
        
        int[] arr = new int[]{1, 3, 3, 7, 8};
        System.out.println("pos:" + Arrays.binarySearch(arr, 4));
        System.setProperty("org.openjdk.java.util.stream.tripwire", "true");
        System.out.println(((4096 >> 3) - 1) >> 1);
        int vol = 30000000;
        LongBtree btree = new LongBtree(vol, 4096, layout -> MemorySegment.allocateNative(layout));
        System.out.println("size: " + btree.memSize());
        System.gc();
        int j = 0;
        Iterator ite4 = new Iterator();
        Iterator ite3 = new Iterator();
        while (j < 100000) {
            btree.reset();
            int k = 0;
            long start = System.currentTimeMillis();
            while (k < vol) {
                btree.put(k, k * 10);
                k++;
            }
            btree.complete();
            long end = System.currentTimeMillis();
            //   System.out.println("write:" + (end - start));
            start = System.currentTimeMillis();
            k = 0;
            while (k < vol) {
                Iterator ite2 = btree.findEq(ite4, k);
                int pos = ite2.nextInt();
                long v = btree.getValue(pos);
                long _key = btree.getKey(pos);
                if (v != _key * 10) {
                    System.out.println("invalid val:" + v + " k : " + k);
                }
                k++;
            }
            end = System.currentTimeMillis();
            //  System.out.println("read:" + (end - start) + "ops:" + (((double) (end - start) * 1000)) / vol);

            Iterator ite = btree.findGE(ite3, 100);
            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));
            
            ite = btree.findGT(ite3, 100);
            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));
            ite = btree.findLT(ite3, 100);
            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));
            ite = btree.findLE(ite3, 100);
            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));
            
            j++;
        }
        
        int k = 0;
        long start = System.currentTimeMillis();
        while (k < vol) {
            long key = btree.getKey(k);
            if (key * 10 != btree.getValue(k)) {
                throw new BTreeException("invalid value :" + key);
            }
            k++;
        }
        long end = System.currentTimeMillis();
        System.out.println("done: " + (end - start));
//        SequenceLayout btree = MemoryLayout.ofSequence(5,
//                MemoryLayout.ofUnion(
//                        MemoryLayout.ofSequence(64,MemoryLayout.ofValueBits(64,ByteOrder.nativeOrder()).withName("keys"),MemoryLayout.ofPaddingBits(64)).withName("keyBlock"),
//                        MemoryLayout.ofValueBits(64, ByteOrder.nativeOrder()).withName("values")
//                ).withName("node")
//        ).withName("btree");
//int nodeCount = 2;
//int nodeItemCount = 1;
//        SequenceLayout mainLayout = MemoryLayout.ofSequence(nodeCount,
//                MemoryLayout.ofUnion(
//                        MemoryLayout.ofStruct(MemoryLayout.ofSequence(nodeItemCount, MemoryLayout.ofValueBits(32, ByteOrder.nativeOrder())).withName("keys"),MemoryLayout.ofPaddingBits(nodeItemCount * 32)).withName("keysBlock"),
//                         MemoryLayout.ofStruct(MemoryLayout.ofPaddingBits(nodeItemCount * 32),MemoryLayout.ofSequence(nodeItemCount, MemoryLayout.ofValueBits(32, ByteOrder.nativeOrder())).withName("values")).withName("valueBlock")
//                ).withName("node"));
//        MemoryAddress address = MemorySegment.allocateNative(mainLayout).baseAddress();
//        
//        VarHandle keyHandler = mainLayout.select(MemoryLayout.PathElement.sequenceElement(),MemoryLayout.PathElement.groupElement("keysBlock"),MemoryLayout.PathElement.groupElement("keys"),MemoryLayout.PathElement.sequenceElement()).varHandle(int.class);
//                //mainLayout.varHandle(int.class, MemoryLayout.PathElement.sequenceElement(),MemoryLayout.PathElement.groupElement("keysBlock"),MemoryLayout.PathElement.groupElement("keys"),MemoryLayout.PathElement.sequenceElement());
//        VarHandle valueHandler = mainLayout.varHandle(int.class, MemoryLayout.PathElement.sequenceElement(),MemoryLayout.PathElement.groupElement("valueBlock"),MemoryLayout.PathElement.groupElement("values"),MemoryLayout.PathElement.sequenceElement());
//        
//        keyHandler.set(address,4);
//        valueHandler.set(address,0,0,8);
//        
//         keyHandler.set(address.addOffset(4),10);
//        
        //     keyHandler.set(address,0,1,12L);
        //      valueHandler.set(address,0,1,24);
//        
//         keyHandler.set(address,0,2,123L);
//        valueHandler.set(address,0,2,345);
//        
//          keyHandler.set(address,0,2,20);
//        valueHandler.set(address,0,2,30);
//        
//            keyHandler.set(address,0,3,12L);
//        valueHandler.set(address,0,3,24);
//        
//        
//              keyHandler.set(address,0,4,12L);
//        valueHandler.set(address,0,4,24);
        //System.out.println("key:"+keyHandler.get(address) + " value : "+valueHandler.get(address,0,0));
        // System.out.println("key:"+keyHandler.get(address,0,1) + " value : "+valueHandler.get(address,0,1));
//        SequenceLayout main = MemoryLayout.ofSequence(64, MemoryLayout.ofValueBits(32, ByteOrder.nativeOrder()));
//        MemorySegment mainSeg = MemorySegment.allocateNative(main);
//        VarHandle valueLayout = main.varHandle(int.class, MemoryLayout.PathElement.sequenceElement());
//        MemorySegment memSeg = MemorySegment.allocateNative(1024 * 1024);
//        VarHandle longHandle = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());
//        VarHandle intHandle = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());
//        VarHandle shortHandle = MemoryHandles.varHandle(short.class, ByteOrder.nativeOrder());
//        MemoryAddress addr = mainSeg.baseAddress();
//        longHandle.set(memSeg.baseAddress(), 12L);
//        long i = 0;
//        while (true) {
//            valueLayout.set(addr, i % 64, 123);
//            Thread.onSpinWait();
//            i++;
//        }

//        intHandle.set(memSeg.baseAddress().addOffset(12),43);
//        
//        System.out.println("long: "+longHandle.get(memSeg.baseAddress()));
//        System.out.println("int: "+intHandle.get(memSeg.baseAddress().addOffset(8)));
//        System.out.println("short: "+intHandle.get(memSeg.baseAddress().addOffset(12)));
    }
}
