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
public class LeafNodeImpl extends ByteBufNode implements LeafNode, WritableLeafNode {

  public LeafNodeImpl(
      ByteBuffer mainBuffer, LongBuffer buffer, int nodeItemCount, int pageSize, int offset) {
    super(mainBuffer, buffer, nodeItemCount, pageSize, offset);
  }

  @Override
  public long getValue(int index) {
    return this.buffer.get(this.nodeItemCount + index);
  }

  @Override
  public long find(long key) {
    return 0;
  }

  @Override
  public void setValue(int index, long value) {
    this.buffer.put(this.nodeItemCount + index, value);
  }

  @Override
  public void setKey(int index, long key) {
    this.buffer.put(index, key);
  }

  @Override
  public void setSize(int size) {
    this.buffer.put((this.nodeItemCount * 2) + 1, size);
  }

  @Override
  public void setNext() {
    long l = this.buffer.get(this.nodeItemCount * 2 + 2);
    this.buffer.put(this.nodeItemCount * 2 + 2, l | 0x1);
  }

  @Override
  public void setPrevious() {
    long l = this.buffer.get(this.nodeItemCount * 2 + 2);
    this.buffer.put(this.nodeItemCount * 2 + 2, l | (0x2));
  }
}
