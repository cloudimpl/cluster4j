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

/** @author nuwansa */
public interface LeafNode extends Node {

  long getValue(int index);

  default Iterator iterator(int index) {
    return new Iterator(this, index);
  }

  public static final class Iterator implements java.util.Iterator<Entry> {

    private final LeafNode current;
    private int index;

    public Iterator(LeafNode current, int index) {
      this.current = current;
      this.index = index;
    }

    @Override
    public boolean hasNext() {
      return index < current.size();
    }

    @Override
    public Entry next() {
      Entry e = new Entry(this.current.getKey(index), this.current.getValue(index));
      this.index++;
      return e;
    }
  }
}
