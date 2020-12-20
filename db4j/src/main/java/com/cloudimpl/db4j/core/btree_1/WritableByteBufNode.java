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
package com.cloudimpl.db4j.core.btree_1;

import java.nio.LongBuffer;

/** @author nuwansa */
public class WritableByteBufNode extends ByteBufNode {

  public WritableByteBufNode(LongBuffer buffer) {
    super(buffer);
  }

  public void setLeft(int index, long value) {
    this.buffer.put(getKeyCapacity() + index, value);
  }

  public void setRight(int index, long value) {
    this.buffer.put(getKeyCapacity() + (index + 1), value);
  }

  public void setKey(int index, long key) {
    this.buffer.put(index, key);
  }

  public void setNext(ByteBufNode node) {
    this.buffer.put(
        this.getFlagPos(),
        (((long) this.getPrevious()) << 32) | (node.buffer.arrayOffset() & 0xFFFFFFFFL));
  }

  public void setPrevious(ByteBufNode node) {
    buffer.put(
        getFlagPos(), (((long) node.buffer.arrayOffset()) << 32) | (this.getNext() & 0xFFFFFFFFL));
  }

  public void setSize(int size) {
    this.buffer.put(this.buffer.limit() - 2, size);
  }
}
