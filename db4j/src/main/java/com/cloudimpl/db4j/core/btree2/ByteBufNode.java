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
package com.cloudimpl.db4j.core.btree2;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/** @author nuwansa */
public class ByteBufNode {

  protected final LongBuffer buffer;
  protected final ByteBuffer mainBuffer;
  protected final int nodeItemCount;
  protected final int pageSize;
  protected final int offset;

  public ByteBufNode(
      ByteBuffer mainBuffer, LongBuffer buffer, int nodeItemCount, int pageSize, int offset) {
    this.mainBuffer = mainBuffer;
    this.buffer = buffer;
    this.nodeItemCount = nodeItemCount;
    this.pageSize = pageSize;
    this.offset = offset;
  }

  public LeafNode next() {
    int exist = (int) (this.buffer.get(this.nodeItemCount * 2 + 2) & 0x1L);
    if (exist == 1) {
      return new LeafNodeImpl(
          mainBuffer,
          mainBuffer.position(offset + pageSize).slice().asLongBuffer().limit(buffer.limit()),
          nodeItemCount,
          pageSize,
          offset + pageSize);
    }
    return null;
  }

  public LeafNode previous() {
    int exist = (int) ((this.buffer.get((this.nodeItemCount * 2) + 2)) & 0x2L) >> 1;
    if (exist == 1) {
      return new LeafNodeImpl(
          mainBuffer,
          mainBuffer.position(offset - pageSize).slice().asLongBuffer().limit(buffer.limit()),
          nodeItemCount,
          pageSize,
          offset - pageSize);
    }
    return null;
  }

  public static Node.Type getType(ByteBuffer mainBuffer, int offset) {
    int type = (int) ((mainBuffer.getLong(offset) >> 16) & 0xFFL);
    switch (type) {
      case 1:
        return Node.Type.LEAF;
      case 2:
        return Node.Type.INDEX;
      default:
        throw new RuntimeException("invalid node type:" + type);
    }
  }

  public long getKey(int index) {
    return buffer.get(index);
  }

  public int size() {
    return (int) this.buffer.get((this.nodeItemCount * 2) + 1);
  }

  public boolean isFull() {
    return size() == this.nodeItemCount;
  }

  public static int binarySearch(LongBuffer buffer, int fromIndex, int toIndex, long key) {
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

  public static int capacity(int pageSize) {
    return pageSize / 8;
  }
}
