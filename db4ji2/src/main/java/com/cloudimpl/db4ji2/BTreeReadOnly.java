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
import java.util.function.Function;
import java.util.stream.StreamSupport;

/** @author nuwansa */
public class BTreeReadOnly implements Queryable {
  protected int maxItemCount;
  protected int pageSize;
  protected int maxItemPerNode;
  protected int leafNodeCounts;
  protected int indexNodeCount;
  protected ByteBuffer mainBuffer;
  protected ReadOnlyNode rootNode;
  private final BTreeReadOnly.Header readOnlyHeader;
  protected final Function<Integer, ByteBuffer> bufferProvider;

  public BTreeReadOnly(
      BTreeReadOnly.Header readOnlyHeader, Function<Integer, ByteBuffer> bufferProvider) {
    this.readOnlyHeader = readOnlyHeader;
    this.bufferProvider = bufferProvider;
  }

  protected void init() {
    this.maxItemCount = this.readOnlyHeader.getMaxItemCount();
    int offset = this.readOnlyHeader.getRootOffset();
    this.pageSize = this.readOnlyHeader.getPageSize();
    this.maxItemPerNode = getMaxItemPerNode(pageSize);
    this.leafNodeCounts = getLeafNodeCount(maxItemCount, maxItemPerNode);
    this.indexNodeCount = getTotalIndexNodeCount(this.leafNodeCounts, maxItemPerNode);
    this.mainBuffer = bufferProvider.apply(pageSize * (this.leafNodeCounts + this.indexNodeCount));
    this.rootNode =
        new ReadOnlyIndexNode(
            mainBuffer, readOnlyHeader.getRootOffset(), this.maxItemPerNode, pageSize);
  }

  public int rootOffset() {
    return this.rootNode.getOffset();
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getMaxItemCount() {
    return maxItemCount;
  }

  @Override
  public int getSize() {
    return readOnlyHeader.getSize();
  }

  public long find(long key) {
    return this.rootNode.find(key);
  }

  protected final int getMaxItemPerNode(int pageSize) {
    return ((pageSize / 8) - 1) / 2;
  }

  protected final int getLeafNodeCount(int maxItemCount, int maxItemPerNode) {
    return (int) Math.ceil(((double) maxItemCount) / maxItemPerNode);
  }

  protected final int getIndexNodeCount(int maxItemCount, int maxItemPerNode) {
    int count = maxItemCount / maxItemPerNode;
    if ((count * maxItemPerNode) + 1 == maxItemCount) {
      return count;
    } else {
      return (int) Math.ceil(((double) maxItemCount) / maxItemPerNode);
    }
  }

  protected final int getTotalIndexNodeCount(int maxItemCount, int maxItemPerNode) {
    int count = getIndexNodeCount(maxItemCount, maxItemPerNode);
    if (count <= 1) {
      return 1;
    }

    return count + getTotalIndexNodeCount(count, maxItemPerNode);
  }

  @Override
  public java.util.Iterator<Entry> findGE(long key) {
    LeafNode.Iterator ite = this.rootNode.findGe(key);
    return new BTree.Iterator(ite.getNode(), ite.getIndex());
  }

  @Override
  public java.util.Iterator<Entry> findGT(long key) {
    LeafNode.Iterator ite = this.rootNode.findGe(key);
    Iterable<Entry> it = () -> new BTree.Iterator(ite.getNode(), ite.getIndex());
    return StreamSupport.stream(it.spliterator(), false).filter(e -> e.getKey() != key).iterator();
  }

  @Override
  public java.util.Iterator<Entry> findLE(long key) {
    LeafNode.Iterator ite = this.rootNode.findLe(key);
    Iterable<Entry> it = () -> new BTree.Iterator(ite.getNode(), ite.getIndex()).reverse(true);
    return StreamSupport.stream(it.spliterator(), false).iterator();
  }

  @Override
  public java.util.Iterator<Entry> findLT(long key) {
    LeafNode.Iterator ite = this.rootNode.findLe(key);
    Iterable<Entry> it = () -> new BTree.Iterator(ite.getNode(), ite.getIndex()).reverse(true);
    return StreamSupport.stream(it.spliterator(), false).filter(e -> e.getKey() != key).iterator();
  }

  @Override
  public java.util.Iterator<Entry> all(boolean asc) {
    return new BTree.Iterator(
        new ReadOnlyLeafNode(mainBuffer, pageSize * indexNodeCount, maxItemPerNode, pageSize), 0);
  }

  public BTree.LeafIterator leafIterator(LeafNode leafNode) {
    return new BTree.LeafIterator(leafNode);
  }

  public BTree.NodeIterator nodeIterator(ReadOnlyNode indexNode) {
    return new BTree.NodeIterator(indexNode);
  }

  @Override
  public java.util.Iterator<Entry> findEQ(long key) {
    LeafNode.Iterator ite = this.rootNode.findGe(key);
    Iterable<Entry> it = () -> new BTree.EqIterator(ite.getNode(), ite.getIndex(), key);
    return StreamSupport.stream(it.spliterator(), false).iterator();
  }

  public static class Iterator implements java.util.Iterator<Entry> {

    private LeafNode leafNode;
    protected LeafNode.Iterator ite;
    private boolean reverse;

    public Iterator(LeafNode leafNode, int index) {
      this.leafNode = leafNode;
      ite = this.leafNode.iterator(index);
      if (!ite.hasNext() && this.leafNode.hasNext()) {
        this.leafNode = this.leafNode.next();
        this.ite = this.leafNode.iterator(0);
      }
      this.reverse = false;
    }

    @Override
    public boolean hasNext() {
      return ite.hasNext();
    }

    @Override
    public Entry next() {
      if (!reverse) {
        return forward();
      } else {
        return backward();
      }
    }

    private Entry forward() {
      Entry e = this.ite.next();
      if (!this.ite.hasNext() && this.leafNode.hasNext()) {
        this.leafNode = this.leafNode.next();
        this.ite = this.leafNode.iterator(0);
      }
      return e;
    }

    private Entry backward() {
      Entry e = this.ite.next();
      if (!this.ite.hasNext() && this.leafNode.hasPrevious()) {
        this.leafNode = this.leafNode.previous();
        this.ite = this.leafNode.iterator(this.leafNode.getSize() - 1).reverse(true);
      }
      return e;
    }

    public Iterator reverse(boolean reverse) {
      this.reverse = reverse;
      this.ite.reverse(reverse);
      if (reverse) {
        if (!ite.hasNext() && this.leafNode.hasPrevious()) {
          this.leafNode = this.leafNode.previous();
          this.ite = this.leafNode.iterator(this.leafNode.getSize() - 1).reverse(reverse);
        }
      }

      return this;
    }
  }

  public static final class EqIterator extends Iterator {

    private long key;

    public EqIterator(LeafNode leafNode, int index, long key) {
      super(leafNode, index);
      this.key = key;
    }

    @Override
    public boolean hasNext() {
      return ite.hasNext() && checkEqual();
    }

    private boolean checkEqual() {
      return this.ite.peek().getKey() == key;
    }
  }

  public static final class LeafIterator implements java.util.Iterator<LeafNode> {

    private LeafNode currentNode;

    public LeafIterator(LeafNode leafNode) {
      this.currentNode = leafNode;
    }

    @Override
    public boolean hasNext() {
      return this.currentNode != null;
    }

    @Override
    public LeafNode next() {
      LeafNode temp = this.currentNode;
      this.currentNode = this.currentNode.next();
      return temp;
    }
  }

  public static final class NodeIterator implements java.util.Iterator<ReadOnlyNode> {

    private ReadOnlyNode currentNode;

    public NodeIterator(ReadOnlyNode currentNode) {
      this.currentNode = currentNode;
    }

    @Override
    public boolean hasNext() {
      return this.currentNode != null;
    }

    @Override
    public ReadOnlyNode next() {
      ReadOnlyNode temp = this.currentNode;
      this.currentNode = this.currentNode.next();
      return temp;
    }
  }

  public static class Header {
    protected final ByteBuffer buffer;

    public Header(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    public int getRootOffset() {
      return this.buffer.getInt(0);
    }

    public int getMaxItemCount() {
      return this.buffer.getInt(4);
    }

    public int getPageSize() {
      return this.buffer.getInt(8);
    }

    public int getSize() {
      return this.buffer.getInt(12);
    }
  }
}