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
public interface WritableIndexNode extends WritableNode {

  void setRightChild(int index, int pos);

  void setLeftChild(int index, int pos);

  default Iterator iterator(int index, int limit) {
    return new Iterator((IndexNodeImpl) this, index, limit);
  }

  public static final class Iterator implements java.util.Iterator<WritableIndexNode> {

    private int index;
    private final int limit;
    private final IndexNodeImpl indexNode;

    public Iterator(IndexNodeImpl indexNode, int index, int limit) {
      this.index = index;
      this.limit = limit;
      this.indexNode = indexNode;
    }

    @Override
    public boolean hasNext() {
      return index < this.limit;
    }

    @Override
    public WritableIndexNode next() {
      WritableIndexNode nextNode =
          new IndexNodeImpl(
              this.indexNode.mainBuffer,
              this.indexNode
                  .mainBuffer
                  .position(this.indexNode.offset + this.indexNode.pageSize)
                  .slice()
                  .asLongBuffer()
                  .limit(this.indexNode.buffer.limit()),
              this.indexNode.nodeItemCount,
              this.indexNode.pageSize,
              this.indexNode.offset + this.indexNode.pageSize);
      index++;
      return nextNode;
    }
  }
}
