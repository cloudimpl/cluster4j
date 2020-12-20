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

/** @author nuwansa */
public abstract class WritableBufNode extends ByteBufNode implements WritableNode {

  public WritableBufNode(ByteBuffer mainBuf, int offset, int capacity, int pageSize, int type) {
    super(mainBuf, offset, capacity, pageSize);
    putType(type);
  }

  @Override
  public void setKey(int index, long key) {
    this.buffer.put(index, key);
  }

  @Override
  public void setValue(int index, long value) {
    this.buffer.put(this.capacity + index, value);
  }

  @Override
  public void setSize(int size) {
    int index = (this.capacity * 2) + 1;
    long val = this.buffer.get(index);
    val = ((size & 0xFFFFFFFFL) << 32) | val;
    this.buffer.put(index, val);
  }

  @Override
  public void setNext() {
    int index = (this.capacity * 2) + 1;
    long val = this.buffer.get(index);
    this.buffer.put(index, val | 0x1L);
  }

  @Override
  public void setPrevious() {
    int index = (this.capacity * 2) + 1;
    long val = this.buffer.get(index);
    this.buffer.put(index, val | 0x2L);
  }

  @Override
  public void putType(int type) {
    int index = (this.capacity * 2) + 1;
    long val = this.buffer.get(index);
    this.buffer.put(index, val & 0xFFFFFFFFFFFFFF3L | type);
  }
}
