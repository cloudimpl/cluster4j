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

/** @author nuwansa */
public class ReadOnlyLeafNode extends ByteBufNode implements LeafNode {

  public ReadOnlyLeafNode(ByteBuffer mainBuf, int offset, int capacity, int pageSize) {
    super(mainBuf, offset, capacity, pageSize);
  }

  @Override
  public long find(long key) {
    int pos = binarySearch(0, getSize(), key);
    if (pos >= 0) {
      return getValue(pos);
    }
    return -1;
  }

  public LeafNode.Iterator findEq(long key) {
    int pos = binarySearch(0, getSize(), key);
    if (pos < 0) return Iterator.Empty;
    return iterator(pos);
  }

  @Override
  public LeafNode.Iterator findGe(long key) {
    int pos = binarySearch(0, getSize(), key);
    if (pos < 0) {
      pos = -pos - 1;
    }
    return iterator(pos);
  }

  @Override
  public Iterator findLe(long key) {
    int pos = binarySearch(0, getSize(), key);
    if (pos < 0) {
      pos = -pos - 2;
    }
    return iterator(pos).reverse(true);
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
}
