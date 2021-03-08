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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.green.jelly.JsonNumber;
import org.green.jelly.MutableJsonNumber;

/**
 *
 * @author nuwan
 */
public class NumberMemBlock implements NumberQueryBlock{
    protected final LongBuffer longBuffer;
    protected final int maxItemCount;
    protected final int offset;
    protected double max;
    protected double min;
    private  long firstValue;
    protected int size = -1;
    protected int pageSize;
    public NumberMemBlock(ByteBuffer byteBuf, int offset, int pageSize) {
        this.pageSize = pageSize;
        this.longBuffer = byteBuf.position(offset).asLongBuffer().limit(pageSize / 8);
        this.offset = offset / 8;
        this.maxItemCount = ((pageSize / 8) - 1) / 2;
    }

    public boolean put(LongBuffer temp,JsonNumber key, long value) {
        int size = getSize();
        if (size == maxItemCount) {
            return false;
        }
        int exp = Math.abs(key.exp());
        if (size == 0) {
            this.firstValue = value;
            long newValue = (value - this.firstValue) << 32 | exp;
            this.longBuffer.put(0, key.mantissa());
            this.longBuffer.put(maxItemCount, newValue);
        } else {
            //      long[] arr = this.longBuffer.array();
            long newValue = (value - this.firstValue) << 32 | exp;
            
            int pos = binarySearch(0, size, key);
            if (pos < 0) {
                pos = -pos - 1;
            }
            temp = temp.limit(offset + size).position(offset + pos);
            this.longBuffer.position(pos + 1);
            //  System.out.println("temp:" + temp.remaining());
            // temp.flip();
            this.longBuffer.put(temp);
            temp = temp.limit(offset + maxItemCount + size).position(offset + pos + maxItemCount);
            this.longBuffer.position(pos + maxItemCount + 1);
            // temp.flip();
            this.longBuffer.put(temp);

            //  System.arraycopy(arr, pos, arr, pos + 1, size - pos);
            // System.arraycopy(arr, pos + maxItemCount, arr, maxItemCount + pos + 1, size - pos);
            this.longBuffer.put(pos, key.mantissa());
            this.longBuffer.put(maxItemCount + pos, newValue);
        }
        size++;
        updateSize(size);
        this.size = size;
        double dkey = key.mantissa() * NumberQueryBlock.lookupTable[exp];
        this.min = Math.min(min, dkey);
        this.max = Math.max(max, dkey);
        return true;
    }

    public boolean isFull()
    {
        return getSize() == maxItemCount;
    }
    
    @Override
    public long getMaxKeyAsLong()
    {
        return getKeyAsLong(size - 1);
    }
    
    @Override
    public long getMinKeyAsLong()
    {
        return getKeyAsLong(0);
    }
    

    @Override
    public long getKeyAsLong(int index) {
        return getKey(index);
    }

     @Override
    public int getMaxExp() {
        return 0;
    }
    
    @Override
    public NumberQueryBlock.Iterator all(NumberQueryBlock.Iterator ite) {
        int size = getSize();
        if (size == 0) {
            return NumberQueryBlock.Iterator.EMPTY;
        }
        return ite.init(this,0,getSize());
    }
    
    protected NumberQueryBlock.Iterator findGE(NumberQueryBlock.Iterator ite,JsonNumber key) {
        int size = getSize();
        int pos = binarySearch(0, size, key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,pos,size);
        } else {
            pos = adjustEqLowPos(pos, key);
            return ite.init(this,pos, getSize());
        }
    }

    protected NumberQueryBlock.Iterator findGT(NumberQueryBlock.Iterator ite,JsonNumber key) {
        int size = getSize();
        int pos = binarySearch(0, size, key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,pos,size);
        } else {
            pos = adjustEqUpperPos(pos, key);
            return ite.init(this,pos, getSize());
        }
    }

    protected NumberQueryBlock.Iterator findLE(NumberQueryBlock.Iterator ite,JsonNumber key) {
        int size = getSize();
        int pos = binarySearch(0, getSize(), key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,0,pos);
        } else {
            pos = adjustEqUpperPos(pos, key);
            return ite.init(this,0, pos);
        }

    }

    protected NumberQueryBlock.Iterator findLT(NumberQueryBlock.Iterator ite,JsonNumber key) {
        int size = getSize();
        int pos = binarySearch(0, getSize(), key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,0,pos);
        } else {
            pos = adjustEqLowPos(pos, key);
            return ite.init(this,0, pos);
        }
    }

    protected NumberQueryBlock.Iterator findEQ(NumberQueryBlock.Iterator ite,JsonNumber key) {
        int size = getSize();
        int pos = binarySearch(0, getSize(), key);
        if (pos >= 0) {
            pos = adjustEqLowPos(pos, key);
            return ite.init(this,pos, size).withEqKey(key.mantissa(),key.exp());
        } else {
            return NumberQueryBlock.Iterator.EMPTY;
        }
    }
    
    private int adjustEqLowPos(int pos, JsonNumber matchKey) {
        int exp = Math.abs(matchKey.exp());
        while (pos >= 0 && compare(pos, matchKey.mantissa(),exp) == 0) {
            pos--;
        }
        return pos + 1;
    }

    private int adjustEqUpperPos(int pos, JsonNumber matchKey) {
        int exp = Math.abs(matchKey.exp());
        while (pos < this.size && compare(pos, matchKey.mantissa(),exp) == 0) {
            pos++;
        }
        return pos;
    }
    
    protected long getKey(int pos)
    {
        return this.longBuffer.get(pos);
    }
    
    private long getRowValue(int pos)
    {
        return this.longBuffer.get(this.maxItemCount + pos);
    }
    
    @Override
    public long getValue(int pos)
    {
        return this.firstValue + (int)(getRowValue(pos) >> 32);
    }
    
    public void updateSize(int size) {
        long val = this.longBuffer.get(this.longBuffer.limit() - 1);
        val = (val & 0xFFFFFFFF00000000L) | size & 0xFFFFFFFFL;
        this.longBuffer.put(this.longBuffer.limit() - 1, val);
        this.size = size;
    }

    @Override
    public int getSize() {
        if (this.size == -1) {
            this.size = (int) this.longBuffer.get(this.longBuffer.limit() - 1);
        }
        return size;
    }

    protected int binarySearch(int fromIndex, int toIndex, JsonNumber key) {
        int low = fromIndex;
        int high = toIndex - 1;
        int keyExp = Math.abs(key.exp());
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midKey = getKey(mid);
            int exp = (int)getRowValue(mid);
            int ret = compare(midKey,exp, key.mantissa(),keyExp);
            if (ret < 0) {
                low = mid + 1;
            } else if (ret > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }
    
    @Override
    public int compare(int index,long rightMantissa,int rightExp)
    {
        long key = getKey(index);
        int exp = (int)getRowValue(index);
        return compare(key, exp, rightMantissa, rightExp);
    }
    
    @Override
    public int compare(long leftMantissa,int leftExp,long rightMantissa,int rightExp)
    {       
        if(leftExp == 0 && rightExp == 0)
        {
            return Long.compare(leftMantissa, rightMantissa);
        }
        return Double.compare(leftMantissa * NumberQueryBlock.lookupTable[leftExp],rightMantissa * NumberQueryBlock.lookupTable[rightExp]);
    }
    
    @Override
    public JsonNumber getKey(int index,MutableJsonNumber json)
    {
         json.set(getKeyAsLong(index),- ((int)getRowValue(index)));
         return json;
    }
     
    @Override
    public String toString() {
        String s = "";
        int i = 0;
        while (i < getSize()) {
            s += this.longBuffer.get(i);
            s += ",";
            i++;
        }
        s += "[";
        i = 0;
        while (i < getSize()) {
            s += this.longBuffer.get(this.maxItemCount + i);
            s += ",";
            i++;
        }
        s += "]";
        return s;
    }
    
    public static JsonNumber[] toJsonNumber(List<BigDecimal> decimals) {
        return decimals.stream().map(d -> new MutableJsonNumber(d.unscaledValue().longValue(), -d.scale())).toArray(JsonNumber[]::new);
    }
    
    public static void main(String[] args) {
        ByteBuffer alloc = ByteBuffer.allocate(4096 * 10);
        NumberMemBlock longBlock = new NumberMemBlock(alloc, 0, 4096);
        LongBuffer temp = alloc.asLongBuffer();
        List<BigDecimal> decimals = IntStream.range(0, 255).mapToObj(i -> new BigDecimal(ThreadLocalRandom.current().nextDouble()).setScale(ThreadLocalRandom.current().nextInt(17), RoundingMode.CEILING)).collect(Collectors.toList());

        List<BigDecimal> list3 = new LinkedList<>(decimals);
        list3.sort(BigDecimal::compareTo);
        System.out.println(list3);
        //   list3.forEach(System.out::println);
        final JsonNumber[] list = toJsonNumber(decimals);//Arrays.asList(IntStream.range(1, 256).boxed().toArray());
        List<JsonNumber> list2 = new LinkedList<>(Arrays.asList(list));
        int i = 0;

        Iterator ite = new Iterator();
        // Collections.shuffle(list2);
        MutableJsonNumber json = new MutableJsonNumber();
        NumberEntry numberEntry = new NumberEntry();
        long start = System.currentTimeMillis();
        while (i < 1000000) {

            longBlock.updateSize(0);

            int q = 0;
            while (q < list2.size()) {

                //  json.set(list2.get(q), 0);
                //    System.out.println("json:"+json);
                //    System.out.println("insert: " + list2.get(q) + " decimal: " + decimals.get(q)+  " row : "+q);
                boolean ok = longBlock.put(temp, list2.get(q), q);
                if (!ok) {
                    System.out.println("Full");
                }
                q++;
            }

//            longBlock.all(ite);
//            q = 0;
//            int j = 0;
//            while (ite.hasNext()) {
//                int pos = ite.nextInt();
//                longBlock.getEntry(numberEntry, pos);
//                long unscale = list3.get(j).unscaledValue().longValue();
//                int exp = list3.get(j).scale();
//
//                int max = Math.max(Math.abs(numberEntry.key.exp()), exp);
//                long unscale2 = unscale * NumberMemBlock.lookupTable[max - exp];
//                long found = numberEntry.key.mantissa() * NumberMemBlock.lookupTable[max - Math.abs(numberEntry.key.exp())];
//                if (unscale2 != found) {
//                    System.out.println("entry:" + numberEntry + "  " + list3.get(j));
//                }
//
////                int j = (int) list.get(q);
////                if (j != numberEntry.key.mantissa()) {
////                    throw new RuntimeException("invalid :" + json + " j:" + j);
////                }
//                j++;
//                q++;
//            }
//            
//            json.set(5, -1);
//            Iterator ite2 = longBlock.findLE(ite, json);
//            ite2.forEachRemaining((int k)->System.out.println(longBlock.getEntry(numberEntry, k)));
//            //   System.out.println("q"+q);
//            if (true) {
//                break;
//            }
            i++;
        }
        long end = System.currentTimeMillis();
        System.out.println("op/s" + ((double) (end - start) * 1000) / (i * list2.size()));
        System.out.println("longBuf: " + longBlock);
    }

    @Override
    public long memSize() {
        return pageSize;
    }

    @Override
    public void close() {
       
    }
}
