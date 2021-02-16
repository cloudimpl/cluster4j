/*
 * Copyright 2020 nuwansa.
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

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 * @author nuwansa
 */
public class LongMemBlock extends AbstractLongMemBlock implements NumberQueryBlock {

    public LongMemBlock(ByteBuffer byteBuf, int offset, int pageSize) {
        super(byteBuf, offset, pageSize);
        setComparator(Long::compare);
    }

    protected boolean put(LongBuffer temp, long key, long value) {
        int size = getSize();
        if (size == maxItemCount) {
            return false;
        }
        if (size == 0) {
            this.longBuffer.put(0, key);
            this.longBuffer.put(maxItemCount, value);
        } else {
            //      long[] arr = this.longBuffer.array();
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
            this.longBuffer.put(pos, key);
            this.longBuffer.put(maxItemCount + pos, value);
        }
        size++;
        updateSize(size);
        this.size = size;
        this.min = Math.min(min, key);
        this.max = Math.max(max, key);
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
    public final void updateSize(int size) {
        super.updateSize(size);
    }

    @Override
    public final int getSize() {
        return super.getSize();
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

//    public static void main(String[] args) {
//        ByteBuffer alloc = ByteBuffer.allocate(4096 * 10);
//        LongMemBlock longBlock = new LongMemBlock(alloc, 4096 * 2, 4096);
//        LongBuffer temp = alloc.position(4096 * 2).asLongBuffer();
//        final List list = Arrays.asList(IntStream.range(1, 256).boxed().toArray());
//        List<Integer> list2 = new LinkedList<>(list);
//        int i = 0;
//
//        LongEntry entry = new LongEntry(1, 1);
//        Iterator ite = longBlock.iterator(0, () -> entry);
//        Collections.shuffle(list2);
//        while (i < 100000000) {
//
//            longBlock.updateSize(0);
//            ite.reset(0);
//
//            int q = 0;
//            while (q < list2.size()) {
//                longBlock.put(temp, list2.get(q), list2.get(q) * 10);
//                q++;
//            }
//
//            q = 0;
//            while (ite.hasNext()) {
//                LongEntry e = ite.next();
//                int j = (int) list.get(q);
//                if (j != e.getKey() || e.getValue() != j * 10) {
//                    throw new RuntimeException("invalid :" + e + " j:" + j);
//                }
//                q++;
//            }
//            //   System.out.println("q"+q);
//
//            i++;
//        }
//
//        System.out.println("longBuf: " + longBlock);
//    }
}
