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
public class WritableIndexNode extends WritableBufNode implements IndexNode {

  public WritableIndexNode(ByteBuffer mainBuf, int offset, int capacity, int pageSize) {
    super(mainBuf, offset, capacity, pageSize, ReadOnlyNode.INDEX);
  }

  public WritableIndexNode createNext() {
    WritableIndexNode next = new WritableIndexNode(mainBuf, offset + pageSize, capacity, pageSize);
    setNext();
    return next;
  }

  @Override
  public ReadOnlyIndexNode next() {
    if (hasNext()) {
      return new ReadOnlyIndexNode(mainBuf, offset + pageSize, capacity, pageSize);
    }
    return null;
  }

  @Override
  public long find(long key) {
    ReadOnlyNode node;
    int pos = binarySearch(0, getSize(), key);
    if (pos >= 0) {
      node = getNode((int) getValue(pos + 1));
    } else {
      pos = (-pos - 1);
      node = getNode((int) getValue(pos));
    }
    return node.find(key);
  }

  @Override
  public LeafNode.Iterator findEq(long key) {
    ReadOnlyNode node;
    int pos = binarySearch(0, getSize(), key);
    if (pos >= 0) {
      node = getNode((int) getValue(pos + 1));
    } else {
      pos = (-pos - 1);
      node = getNode((int) getValue(pos));
    }
    return node.findEq(key);
  }

  @Override
  public LeafNode.Iterator findGe(long key) {
    ReadOnlyNode node;
    int pos = binarySearch(0, getSize(), key);
    if (pos >= 0) {
      node = getNode((int) getValue(pos + 1));
    } else {
      pos = (-pos - 1);
      node = getNode((int) getValue(pos));
    }
    return node.findGe(key);
  }

  @Override
  public LeafNode.Iterator findLe(long key) {
    ReadOnlyNode node;
    int pos = binarySearch(0, getSize(), key);
    if (pos >= 0) {
      node = getNode((int) getValue(pos + 1));
    } else {
      pos = (-pos - 1);
      node = getNode((int) getValue(pos));
    }
    return node.findLe(key);
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
