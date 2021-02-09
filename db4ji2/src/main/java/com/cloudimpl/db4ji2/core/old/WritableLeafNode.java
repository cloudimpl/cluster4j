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
package com.cloudimpl.db4ji2.core.old;

import com.cloudimpl.db4ji2.core.old.ReadOnlyNode;
import com.cloudimpl.db4ji2.core.old.ReadOnlyLeafNode;
import com.cloudimpl.db4ji2.core.old.ReadOnlyIndexNode;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.Supplier;

/** @author nuwansa */
public class WritableLeafNode extends WritableBufNode implements LeafNode {

  public WritableLeafNode(ByteBuffer mainBuf, int offset, int capacity, int pageSize) {
    super(mainBuf, offset, capacity, pageSize, ReadOnlyNode.LEAF);
  }

  @Override
  public long find(LongComparable comparator,long key) {
    int pos = binarySearch(0, getSize(), key,comparator);
    if (pos >= 0) return getValue(pos);
    return -1;
  }

  @Override
  public LeafNode.Iterator findEq(LongComparable comparator,Supplier<? extends Entry> entrySupplier,long key) {
    int pos = binarySearch(0, getSize(), key,comparator);
    if (pos < 0) {
      return LeafNode.Iterator.Empty;
    }
    return iterator(pos,entrySupplier);
  }

  @Override
  public LeafNode.Iterator findGe(LongComparable comparator,Supplier<? extends Entry> entrySupplier,long key) {
    int pos = binarySearch(0, getSize(), key,comparator);
    if (pos < 0) {
      pos = -pos - 1;
      return iterator(pos,entrySupplier);
    }else
    {
        return iterator(pos,entrySupplier).hasEq();
    }
  }

  @Override
  public Iterator findLe(LongComparable comparator,Supplier<? extends Entry> entrySupplier,long key) {
    int pos = binarySearch(0, getSize(), key,comparator);
    if (pos < 0) {
      pos = -pos - 2;
      return iterator(pos,entrySupplier).reverse(true);
    }else
    {
        return iterator(pos,entrySupplier).hasEq().reverse(true);
    }
  }

  public WritableLeafNode createNext() {
    WritableLeafNode next =
        new WritableLeafNode(mainBuf, offset + this.pageSize, capacity, pageSize);
    setNext();
    return next;
  }

  @Override
  public ReadOnlyLeafNode next() {
    if (hasNext()) {
      return new ReadOnlyLeafNode(mainBuf, offset + pageSize, capacity, pageSize);
    }
    return null;
  }

  @Override
  public ReadOnlyLeafNode previous() {
    if (hasPrevious()) {
      return new ReadOnlyLeafNode(mainBuf, offset - pageSize, capacity, pageSize);
    }
    return null;
  }

  private ReadOnlyNode getNode(int location) {
    int type = ByteBufNode.getType(mainBuf, location, capacity);
    switch (type) {
      case ReadOnlyNode.LEAF:
        {
          return new ReadOnlyLeafNode(mainBuf, location, capacity, pageSize);
        }
      case ReadOnlyNode.INDEX:
        {
          return new ReadOnlyIndexNode(mainBuf, location, capacity, pageSize);
        }
      default:
        {
          throw new RuntimeException("unknown node");
        }
    }
  }
}
