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
import org.green.jelly.JsonNumber;
import org.green.jelly.MutableJsonNumber;

/**
 *
 * @author nuwan
 */
public class DoubleBtree extends AbstractBTree {

    private int maxExp;
    public DoubleBtree(int maxItemCount, int pageSize, Function<MemoryLayout, MemorySegment> memoryProvider) {
        super(maxItemCount, pageSize, Long.BYTES, long.class, Double.BYTES, double.class, Long.BYTES, long.class, memoryProvider);
    }

    @Override
    protected void fillIdxNode(long dstNodeIdx, long dstItemIdx, VarHandle itemHandler, long srcNodeIdx, long srcItemIdx) {
        double key;
        if (itemHandler == keyItemHandler) {
            long mantissa = (long) itemHandler.get(this.address, srcNodeIdx, srcItemIdx);
            int exp = (int) ((long) getRowValue(((int)srcNodeIdx) * this.keyNodeCapacity + (int)srcItemIdx));
            key = mantissa * NumberQueryBlock.lookupTable2[exp];
        } else {
            key = (double) itemHandler.get(this.address, srcNodeIdx, srcItemIdx);
        }
        idxItemHandler.set(address, dstNodeIdx, dstItemIdx, key);
    }

    public void put(JsonNumber key, long value) {
        if (this.currentItemIndex >= this.maxItemCount) {
            throw new BTreeException("btree is full:" + this.currentItemIndex);
        }
        long keyNodeIdx = currentItemIndex >> this.keyIdxExponent;
        long KeyitemIdx = currentItemIndex & (this.keyNodeCapacity - 1);

        int exp = Math.abs(key.exp());
        this.maxExp = Math.max(this.maxExp, exp);
        this.keyItemHandler.set(this.address, keyNodeIdx, KeyitemIdx, key.mantissa());

        if (this.currentItemIndex == 0) {
            this.minKeyHandler.set(this.address, key.mantissa() * NumberQueryBlock.lookupTable2[exp]);
            this.setStartingOffset(value);
        }
        long newVal = (value - getStartingOffset()) << 32 | exp;
        setValue(newVal);
        this.currentItemIndex++;
        if (this.currentItemIndex == this.maxItemCount) {
            this.maxKeyHandler.set(this.address, key.mantissa() * NumberQueryBlock.lookupTable2[exp]);
        }
    }

    @Override
    protected final void setValue(long value) {
        long valueNodeIdx = currentItemIndex >> this.valueExponent;
        long valueitemIdx = currentItemIndex & (this.valueNodeCapacity - 1);
        this.valueItemHandler.set(this.address, valueNodeIdx, valueitemIdx, value);
    }

    @Override
    public long getValue(int index) {
        int nodeIdx = index >> this.valueExponent;
        int itemIdx = index & this.valueNodeCapacity - 1;
        return (long) getStartingOffset() + ((int) ((long)this.valueItemHandler.get(this.address, nodeIdx, itemIdx) >> 32));
    }
    
    @Override
    public void complete() {
       this.maxExpHandler.set(this.address,(long)maxExp);
       super.complete();
    }
    
    @Override
    public long getMaxKeyAsLong() {
        return (long) this.maxKeyHandler.get(this.address);
    }

    @Override
    public long getMinKeyAsLong() {
        return (long) this.minKeyHandler.get(this.address);
    }

    public final Iterator findEq(Iterator ite, JsonNumber key) {
        int leafNodeIdx = findLeafNode(0, 0, key);
        int size = Math.min(this.currentItemIndex - (leafNodeIdx * this.keyNodeCapacity), this.keyNodeCapacity);
        int pos = binarySearch(keyItemHandler, leafNodeIdx, size, key);
        if (pos >= 0) {
            pos = adjustEqLowPos((leafNodeIdx * this.keyNodeCapacity) + pos, key);
            return ite.withEqKey(key.mantissa(), key.exp()).init(this, pos, getSize());
        }
        return Iterator.EMPTY;
    }

    public Iterator findGE(Iterator ite, JsonNumber key) {
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

    public Iterator findGT(Iterator ite, JsonNumber key) {
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

    public Iterator findLE(Iterator ite, JsonNumber key) {
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

    public Iterator findLT(Iterator ite, JsonNumber key) {
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

    private long getKey(VarHandle itemHandler, int nodeIdx, int itemIdx) {
        return (long) itemHandler.get(this.address, nodeIdx, itemIdx);
    }

    private double getKeyAsDouble(VarHandle itemHandler, int nodeIdx, int itemIdx) {
        return (double) itemHandler.get(this.address, nodeIdx, itemIdx);
    }

    private int adjustEqLowPos(int pos, JsonNumber matchKey) {
        int exp = Math.abs(matchKey.exp());
        while (pos >= 0 && compare(pos, matchKey.mantissa(), exp) == 0) {
            pos--;
        }
        return pos + 1;
    }

    private int adjustEqUpperPos(int pos, JsonNumber matchKey) {
        int exp = Math.abs(matchKey.exp());
        while (pos < this.currentItemIndex && compare(pos, matchKey.mantissa(), exp) == 0) {
            pos++;
        }
        return pos;
    }

    private long getKey(int index) {
        int nodeIdx = index >> this.keyIdxExponent;
        int itemIdx = index & this.keyNodeCapacity - 1;
        return (long) this.keyItemHandler.get(this.address, nodeIdx, itemIdx);
    }

    @Override
    public int compare(int index, long rightMantissa, int rightExp) {
        long left = getKey(index);
        int leftExp = (int) getRowValue(index);

        if (leftExp == 0 && rightExp == 0) {
            return Long.compare(left, rightMantissa);
        }
        return Double.compare(left * NumberQueryBlock.lookupTable2[leftExp], rightMantissa * NumberQueryBlock.lookupTable2[rightExp]);
    }

    protected int binarySearch(VarHandle itemHandler, int nodeIdx, int size, JsonNumber key) {
       if(itemHandler == keyItemHandler)
           return binaryLongSearch(itemHandler, nodeIdx, size, key);
       else
           return binaryDoubleSearch(itemHandler, nodeIdx, size, key);
    }

    private int binaryLongSearch(VarHandle itemHandler, int nodeIdx, int size, JsonNumber key) {
        int low = 0;
        int high = size - 1;
        int keyExp = Math.abs(key.exp());
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long mantissa = getKey(itemHandler, nodeIdx, mid);
            int  exp = (int)((long)valueItemHandler.get(this.address,(nodeIdx * this.keyNodeCapacity) >> this.valueExponent,0));
            int ret = compare(mantissa, exp, key.mantissa(), keyExp);
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

    private int binaryDoubleSearch(VarHandle itemHandler, int nodeIdx, int size, JsonNumber key) {
        int low = 0;
        int high = size - 1;
        int exp = Math.abs(key.exp());
        while (low <= high) {
            int mid = (low + high) >>> 1;
            double midVal = getKeyAsDouble(itemHandler, nodeIdx, mid);
            int ret = Double.compare(midVal, key.mantissa() * NumberQueryBlock.lookupTable2[exp]);
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

    private int findLeafNode(int level, int itemIdx, JsonNumber key) {
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
        return maxExp;
    }

    public static void main(String[] args) {

        int[] arr = new int[]{1, 3, 3, 7, 8};
        System.out.println("pos:" + Arrays.binarySearch(arr, 4));
        System.setProperty("org.openjdk.java.util.stream.tripwire", "true");
        System.out.println(((4096 >> 3) - 1) >> 1);
        int vol = 30000000;
        DoubleBtree btree = new DoubleBtree(vol, 4096, layout -> MemorySegment.allocateNative(layout));
        System.out.println("size: " + btree.memSize());
        System.gc();
        int j = 0;
        Iterator ite4 = new Iterator();
        Iterator ite3 = new Iterator();
        MutableJsonNumber json = new MutableJsonNumber();
 
        while (j < 100000) {
            btree.reset();
            int k = 0;
            long start = System.currentTimeMillis();
            while (k < vol) {
                json.set(k, 0);
                btree.put(json, k * 10);
                k++;
            }
            btree.complete();
            long end = System.currentTimeMillis();
            //   System.out.println("write:" + (end - start));
            start = System.currentTimeMillis();
            k = 0;
            while (k < vol) {
                json.set(k, 0);
                Iterator ite2 = btree.findEq(ite4, json);
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
            json.set(100, 0);
            Iterator ite = btree.findGE(ite3, json);
            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));

            ite = btree.findGT(ite3, json);
            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));
            ite = btree.findLT(ite3, json);
            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));
            ite = btree.findLE(ite3, json);
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
