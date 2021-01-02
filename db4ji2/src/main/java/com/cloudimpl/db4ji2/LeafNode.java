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

/** @author nuwansa */
public interface LeafNode {

  long getKey(int index);

  long getValue(int index);

  int getSize();

  boolean hasNext();

  boolean hasPrevious();

  ReadOnlyLeafNode next();

  ReadOnlyLeafNode previous();

  default Iterator iterator(int index) {
    return new Iterator(this, index);
  }

  public static final class Iterator implements java.util.Iterator<Entry> {
    private final LeafNode node;
    private int index;
    private boolean reverse;

    public static final Iterator Empty = new Iterator(null, 0);

    public Iterator(LeafNode node, int index) {
      this.node = node;
      this.index = index;
      this.reverse = false;
    }

    @Override
    public boolean hasNext() {
      if (reverse) {
        return index >= 0;
      }
      return this.node != null && index < this.node.getSize();
    }

    @Override
    public Entry next() {
      Entry e = new Entry(this.node.getKey(index), this.node.getValue(index));
      if (reverse) {
        index--;
      } else {
        index++;
      }
      return e;
    }

    public Entry peek() {
      Entry e = new Entry(this.node.getKey(index), this.node.getValue(index));
      return e;
    }

    public LeafNode getNode() {
      return this.node;
    }

    public int getIndex() {
      return this.index;
    }

    public Iterator reverse(boolean reverse) {
      this.reverse = reverse;
      return this;
    }
  }
}
