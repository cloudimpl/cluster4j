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
package com.cloudimpl.db4ji2.core;

import com.cloudimpl.db4ji2.core.LongEntry;
import com.cloudimpl.db4ji2.core.ReadOnlyLeafNode;
import java.util.function.Supplier;

/** @author nuwansa */
public interface LeafNode {

  long getKey(int index);

  long getValue(int index);

  int getSize();

  boolean hasNext();

  boolean hasPrevious();

  ReadOnlyLeafNode next();

  ReadOnlyLeafNode previous();

  default <T extends Entry> Iterator iterator(int index,Supplier<T> entrySupplier) {
    return new Iterator(this, index,entrySupplier);
  }

   String toString();

  
  public static final class Iterator<T extends Entry> implements java.util.Iterator<T> {
    private final LeafNode node;
    private int index;
    private boolean reverse;
    private Supplier<T> entrySupplier;
    public static final Iterator Empty = new Iterator(null, 0,null);

    protected boolean hasEq;
    public Iterator(LeafNode node, int index,Supplier<T> entrySupplier) {
      this.node = node;
      this.index = index;
      this.reverse = false;
      this.entrySupplier = entrySupplier;
    }

    @Override
    public boolean hasNext() {
      if (reverse) {
        return index >= 0;
      }
      return this.node != null && index < this.node.getSize();
    }

    @Override
    public T next() {
      LongEntry e = this.entrySupplier.get().set(this.node.getKey(index), this.node.getValue(index));
      if (reverse) {
        index--;
      } else {
        index++;
      }
      return (T)e;
    }

    public T peek() {
      T e = this.entrySupplier.get().set(this.node.getKey(index), this.node.getValue(index));
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
    
    protected Iterator hasEq()
    {
        this.hasEq = true;
        return this;
    }
    
    protected boolean isEq()
    {
        return this.hasEq;
    }
  }
}
