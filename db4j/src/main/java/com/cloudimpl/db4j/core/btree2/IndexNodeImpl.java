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
public class IndexNodeImpl extends ByteBufNode implements IndexNode, WritableIndexNode {

  public IndexNodeImpl(
      ByteBuffer mainBuffer, LongBuffer buffer, int nodeItemCount, int pageSize, int offset) {
    super(mainBuffer, buffer, nodeItemCount, pageSize, offset);
  }

  @Override
  public long find(long key) {
    int index = ByteBufNode.binarySearch(buffer, 0, size(), key);
    if (index >= 0) {
      return getRightChild(index).find(key);
    } else {
      index = (-index - 1);
      return getLeftChild(index).find(key);
    }
  }

  private Node getLeftChild(int index) {
    int pos = (int) buffer.get(this.nodeItemCount + index);
    Node.Type type = ByteBufNode.getType(mainBuffer, offset + (this.nodeItemCount * 2 * 8) + 8);
    switch (type) {
      case INDEX:
        {
          return new IndexNodeImpl(
              mainBuffer,
              mainBuffer.position(pos).slice().asLongBuffer().limit(buffer.limit()),
              nodeItemCount,
              pageSize,
              pos);
        }
      case LEAF:
        {
          return new LeafNodeImpl(
              mainBuffer,
              mainBuffer.position(pos).slice().asLongBuffer().limit(buffer.limit()),
              nodeItemCount,
              pageSize,
              pos);
        }
    }
    return null;
  }

  private Node getRightChild(int index) {
    int pos = (int) buffer.get(this.nodeItemCount + index + 1);
    Node.Type type = ByteBufNode.getType(mainBuffer, offset + (this.nodeItemCount * 2 * 8) + 8);
    switch (type) {
      case INDEX:
        {
          return new IndexNodeImpl(
              mainBuffer,
              mainBuffer.position(pos).slice().asLongBuffer().limit(buffer.limit()),
              nodeItemCount,
              pageSize,
              pos);
        }
      case LEAF:
        {
          return new LeafNodeImpl(
              mainBuffer,
              mainBuffer.position(pos).slice().asLongBuffer().limit(buffer.limit()),
              nodeItemCount,
              pageSize,
              pos);
        }
    }
    return null;
  }

  @Override
  public void setRightChild(int index, int pos) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setLeftChild(int index, int pos) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setKey(int index, long key) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setSize(int size) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setNext() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void setPrevious() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }
}
