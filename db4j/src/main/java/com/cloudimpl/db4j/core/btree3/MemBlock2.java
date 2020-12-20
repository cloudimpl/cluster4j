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
package com.cloudimpl.db4j.core.btree3;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;

/** @author nuwansa */
public class MemBlock2 implements Iterable<Entry> {

  private final ByteBuffer buffer;
  private final int maxItemCount;
  private final int pageSize;
  private int size;
  private final LongBuffer longBuffer;
  private MemBlockManager blockMan;
  public static ManyToManyConcurrentArrayQueue<MemBlock2> queue;

  public static void init(int blockCount) {
    queue = new ManyToManyConcurrentArrayQueue<>(blockCount);
    int i = 0;
    while (i < blockCount) {
      queue.add(new MemBlock2(null, 255, 8192, k -> ByteBuffer.allocateDirect(k * 8)));
      i++;
    }
  }

  public static MemBlock2 get() {
    return queue.poll();
  }

  public MemBlock2(
      MemBlockManager blockMan,
      int maxItemCount,
      int pageSize,
      Function<Integer, ByteBuffer> bufferProvider) {
    this.blockMan = blockMan;
    this.maxItemCount = ((pageSize / 8) - 1) / 2;
    this.pageSize = pageSize;
    this.buffer = bufferProvider.apply(pageSize);
    this.size = 0;
    this.longBuffer = this.buffer.asLongBuffer();
  }

  protected int binarySearch(int fromIndex, int toIndex, long key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = longBuffer.get(mid);

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

  public boolean put(long key, long value) {
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
      LongBuffer temp = buffer.asLongBuffer().position(pos).limit(size);
      this.longBuffer.position(pos + 1);
      //  System.out.println("temp:" + temp.remaining());
      // temp.flip();
      this.longBuffer.put(temp);

      temp = buffer.asLongBuffer().position(pos + maxItemCount).limit(maxItemCount + size);
      this.longBuffer.position(pos + maxItemCount + 1);
      // temp.flip();
      this.longBuffer.put(temp);

      //  System.arraycopy(arr, pos, arr, pos + 1, size - pos);
      // System.arraycopy(arr, pos + maxItemCount, arr, maxItemCount + pos + 1, size - pos);
      this.longBuffer.put(pos, key);
      this.longBuffer.put(maxItemCount + pos, value);
    }
    size++;
    return true;
  }

  public Entry entry(int index) {
    return new Entry(this.longBuffer.get(index), this.longBuffer.get(maxItemCount + index));
  }

  public java.util.Iterator<Entry> iterator(int index) {
    return new Iterator(index, this);
  }

  @Override
  public String toString() {
    String s = "";
    int i = 0;
    while (i < size) {
      s += this.longBuffer.get(i);
      s += ",";
      i++;
    }
    s += "[";
    i = 0;
    while (i < size) {
      s += this.longBuffer.get(this.maxItemCount + i);
      s += ",";
      i++;
    }
    s += "]";
    return s;
  }

  @Override
  public java.util.Iterator<Entry> iterator() {
    return iterator(0);
  }

  public static final class Iterator implements java.util.Iterator<Entry> {
    private int index;
    private final MemBlock2 block;

    public Iterator(int index, MemBlock2 block) {
      this.index = index;
      this.block = block;
    }

    @Override
    public boolean hasNext() {
      return index < this.block.size;
    }

    @Override
    public Entry next() {
      Entry e = this.block.entry(index);
      index++;
      return e;
    }
  }

  public static void main(String[] args) {
    int vol = 10_000_000;
    int itemCountPerBlock = 255;
    MemBlock2.init(39216);
    MemBlock2 m =
        MemBlock2.get(); // new MemBlock(itemCountPerBlock, 4096, i -> ByteBuffer.allocate(12));
    List<Integer> list = // Arrays.asList(7, 1, 8, 6, 9, 4, 3, 0, 5, 2);
        IntStream.range(0, vol).boxed().collect(Collectors.toList());
    Collections.shuffle(list);
    System.gc();
    System.out.println("starting");
    // System.out.println("list:" + list);
    int block = 1;
    int i = 0;
    long s = System.currentTimeMillis();
    while (i < list.size()) {
      boolean b = m.put((long) list.get(i), (long) list.get(i) * 10);
      if (!b) {
        m = MemBlock2.get(); // new MemBlock(itemCountPerBlock, 4096, k -> ByteBuffer.allocate(12));
        m.put((long) list.get(i), (long) list.get(i) * 10);
        block++;
      }
      //  System.out.println(m);
      i++;
    }
    System.out.println("ops:" + (((double) list.size() / (System.currentTimeMillis() - s) * 1000)));
    System.out.println("block:" + block);
  }
}
