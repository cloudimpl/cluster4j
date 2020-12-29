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
package com.cloudimpl.db4ji2;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/** @author nuwansa */
public class LongMemBlock {

  private final LongBuffer longBuffer;
  private final int maxItemCount;
  private final int offset;
  private long max;
  private long min;

  public LongMemBlock(ByteBuffer byteBuf, int offset, int pageSize) {
    this.longBuffer = byteBuf.position(offset).asLongBuffer().limit(pageSize / 8);
    this.offset = offset / 8;
    this.maxItemCount = ((pageSize / 8) - 1) / 2;
  }

  public boolean put(LongBuffer temp, long key, long value) {
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
      if (pos <= 0) {
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
    return true;
  }

  public java.util.Iterator<Entry> findGE(long key, Supplier<Entry> sup) {
    int size = getSize();
    if (size == 0 || max < key) {
      return Iterator.EMPTY;
    }
    int pos = binarySearch(0, size, key);
    if (pos < 0) {
      pos = -pos - 1;
    }
    return iterator(pos, sup);
  }

  public java.util.Iterator<Entry> findGT(long key, Supplier<Entry> sup) {
    Iterable<Entry> ite = () -> findGE(key, sup);
    return StreamSupport.stream(ite.spliterator(), false).filter(e -> e.getKey() != key).iterator();
  }

  public java.util.Iterator<Entry> findLE(long key, Supplier<Entry> sup) {
    int size = getSize();
    if (size == 0 || min > key) {
      return Iterator.EMPTY;
    }
    int pos = binarySearch(0, getSize(), key);
    if (pos < 0) {
      pos = -pos - 2;
    }
    return iterator(pos, sup).reverse();
  }

  public java.util.Iterator<Entry> findLT(long key, Supplier<Entry> sup) {
    Iterable<Entry> ite = () -> findLE(key, sup);
    return StreamSupport.stream(ite.spliterator(), false).filter(e -> e.getKey() != key).iterator();
  }

  public java.util.Iterator<Entry> findEQ(long key, Supplier<Entry> sup) {
    int size = getSize();
    if (size == 0 || min > key || max < key) {
      return Iterator.EMPTY;
    }
    int pos = binarySearch(0, getSize(), key);
    if (pos >= 0) {
      return new EqIterator(pos, this, sup, key);
    } else {
      return Iterator.EMPTY;
    }
  }

  public void updateSize(int size) {
    long val = this.longBuffer.get(this.longBuffer.limit() - 1);
    val = (val & 0xFFFFFFFF00000000L) | size & 0xFFFFFFFFL;
    this.longBuffer.put(this.longBuffer.limit() - 1, val);
  }

  public int getSize() {
    return (int) this.longBuffer.get(this.longBuffer.limit() - 1);
  }

  public Entry entry(int index, Supplier<Entry> sup) {
    return sup.get().set(this.longBuffer.get(index), this.longBuffer.get(maxItemCount + index));
  }

  public Iterator iterator(int index, Supplier<Entry> sup) {
    return new Iterator(index, this, sup);
  }

  protected int binarySearch(int fromIndex, int toIndex, long key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = this.longBuffer.get(mid);

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
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

  public static class Iterator implements java.util.Iterator<Entry> {

    protected int index;
    protected final LongMemBlock block;
    protected Supplier<Entry> supplier;
    protected boolean reverse = false;
    public static final Iterator EMPTY = new Iterator(0, null, null);

    public Iterator(int index, LongMemBlock block, Supplier<Entry> supplier) {
      this.index = index;
      this.block = block;
      this.supplier = supplier;
    }

    public void reset(int index) {
      this.index = index;
    }

    public Iterator reverse() {
      this.reverse = !this.reverse;
      return this;
    }

    @Override
    public boolean hasNext() {
      return this.block != null && checkNext();
    }

    private boolean checkNext() {
      if (reverse) {
        return this.index >= 0;
      } else {
        return index < this.block.getSize();
      }
    }

    @Override
    public Entry next() {
      Entry e = this.block.entry(index, this.supplier);
      if (reverse) {
        index--;
      } else {
        index++;
      }
      return e;
    }
  }

  public static final class EqIterator extends Iterator {

    private final long key;

    public EqIterator(int index, LongMemBlock block, Supplier<Entry> supplier, long key) {
      super(index, block, supplier);
      this.key = key;
    }

    @Override
    public boolean hasNext() {
      return super.hasNext() && checkEqual();
    }

    private boolean checkEqual() {
      Entry e = this.block.entry(index, this.supplier);
      return e.getKey() == key;
    }
  }

  public static void main(String[] args) {
    ByteBuffer alloc = ByteBuffer.allocate(4096 * 10);
    LongMemBlock longBlock = new LongMemBlock(alloc, 4096 * 2, 4096);
    LongBuffer temp = alloc.position(4096 * 2).asLongBuffer();
    final List list = Arrays.asList(IntStream.range(1, 256).boxed().toArray());
    List<Integer> list2 = new LinkedList<>(list);
    int i = 0;

    Entry entry = new Entry(1, 1);
    Iterator ite = longBlock.iterator(0, () -> entry);
    Collections.shuffle(list2);
    while (i < 100000000) {

      longBlock.updateSize(0);
      ite.reset(0);

      int q = 0;
      while (q < list2.size()) {
        longBlock.put(temp, list2.get(q), list2.get(q) * 10);
        q++;
      }

      q = 0;
      while (ite.hasNext()) {
        Entry e = ite.next();
        int j = (int) list.get(q);
        if (j != e.getKey() || e.getValue() != j * 10) {
          throw new RuntimeException("invalid :" + e + " j:" + j);
        }
        q++;
      }
      //   System.out.println("q"+q);

      i++;
    }

    System.out.println("longBuf: " + longBlock);
  }
}
