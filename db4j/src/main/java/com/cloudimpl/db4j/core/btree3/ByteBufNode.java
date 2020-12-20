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

/** @author nuwansa */
public abstract class ByteBufNode implements ReadOnlyNode {

  protected final ByteBuffer mainBuf;
  protected final LongBuffer buffer;
  protected final int offset;
  protected final int capacity;
  protected final int pageSize;

  public ByteBufNode(ByteBuffer mainBuf, int offset, int capacity, int pageSize) {
    this.mainBuf = mainBuf;
    this.offset = offset;
    this.buffer = mainBuf.position(offset).slice().asLongBuffer().limit(pageSize / 8);
    this.capacity = capacity;
    this.pageSize = pageSize;
  }

  @Override
  public long getKey(int index) {
    return this.buffer.get(index);
  }

  @Override
  public long getValue(int index) {
    return this.buffer.get(this.capacity + index);
  }

  @Override
  public int getSize() {
    return (int) (this.buffer.get((this.capacity * 2) + 1) >> 32);
  }

  @Override
  public int getCapacity() {
    return this.capacity;
  }

  @Override
  public boolean hasNext() {
    int exist = (int) (this.buffer.get((this.capacity * 2) + 1) & 0x1L);
    return exist == 1;
  }

  @Override
  public boolean hasPrevious() {
    int exist = (int) (this.buffer.get((this.capacity * 2) + 1) & 0x2L) >> 1;
    return exist == 1;
  }

  @Override
  public int getType() {
    return (int) (this.buffer.get((this.capacity * 2) + 1) & 0xCL);
  }

  public static int getType(ByteBuffer buffer, int offset, int capacity) {
    return (int) (buffer.getLong(offset + (capacity * 2 * 8) + 8) & 0xCL);
  }

  @Override
  public int getOffset() {
    return this.offset;
  }

  protected int binarySearch(int fromIndex, int toIndex, long key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = buffer.get(mid);

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
}
