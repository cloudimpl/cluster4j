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
package com.cloudimpl.db4j.core_old;

import java.nio.ByteBuffer;

/** @author nuwansa */
public class Item implements Comparable<Item> {

  private final ByteBuffer buf;

  public Item(ByteBuffer buf) {
    this.buf = buf;
  }

  public long getKey() {
    return this.buf.getLong(0);
  }

  public long getValue() {
    return this.buf.getLong(8);
  }

  @Override
  public String toString() {
    return "Item{" + "key=" + getKey() + ", value=" + getValue() + '}';
  }

  @Override
  public int compareTo(Item o) {
    return Long.compare(getKey(), o.getKey());
  }
}
