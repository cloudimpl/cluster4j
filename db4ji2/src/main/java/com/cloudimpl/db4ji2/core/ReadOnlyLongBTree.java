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

import com.cloudimpl.db4ji2.idx.lng.LongQueryable;
import com.cloudimpl.db4ji2.core.Entry;
import com.cloudimpl.db4ji2.core.LeafNode;
import com.cloudimpl.db4ji2.core.LongComparable;
import com.cloudimpl.db4ji2.core.ReadOnlyNode;
import com.cloudimpl.db4ji2.core.ReadOnlyLeafNode;
import com.cloudimpl.db4ji2.core.ReadOnlyIndexNode;
import com.cloudimpl.db4ji2.core.LongBTree;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

/**
 * @author nuwansa
 */
public class ReadOnlyLongBTree implements LongQueryable {

    protected int maxItemCount;
    protected int pageSize;
    protected int maxItemPerNode;
    protected int leafNodeCounts;
    protected int indexNodeCount;
    protected ByteBuffer mainBuffer;
    protected ReadOnlyNode rootNode;
    private final ReadOnlyLongBTree.Header readOnlyHeader;
    protected final Function<Integer, ByteBuffer> bufferProvider;
    private final LongComparable comparator;
    private final Supplier<? extends Entry> entrySupplier;
    public ReadOnlyLongBTree(
            ReadOnlyLongBTree.Header readOnlyHeader, Function<Integer, ByteBuffer> bufferProvider, LongComparable comparator,Supplier<? extends Entry> entrySupplier) {
        this.readOnlyHeader = readOnlyHeader;
        this.bufferProvider = bufferProvider;
        this.comparator = comparator;
        this.entrySupplier = entrySupplier;
    }

    public void init() {
        this.maxItemCount = this.readOnlyHeader.getMaxItemCount();
        int offset = this.readOnlyHeader.getRootOffset();
        this.pageSize = this.readOnlyHeader.getPageSize();
        this.maxItemPerNode = getMaxItemPerNode(pageSize);
        this.leafNodeCounts = getLeafNodeCount(maxItemCount, maxItemPerNode);
        this.indexNodeCount = getTotalIndexNodeCount(this.leafNodeCounts, maxItemPerNode);
        this.mainBuffer = bufferProvider.apply(pageSize * (this.leafNodeCounts + this.indexNodeCount));
        this.rootNode
                = new ReadOnlyIndexNode(
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
        return this.rootNode.find(this.comparator, key);
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
    public <T extends Entry> java.util.Iterator<T> findGE(long key) {
        LeafNode.Iterator ite = this.rootNode.findGe(this.comparator,entrySupplier, key);
        return new LongBTree.Iterator(ite.getNode(), ite.getIndex(),entrySupplier);
    }

    @Override
    public <T extends Entry> java.util.Iterator<T> findGT(long key) {
        LeafNode.Iterator ite = this.rootNode.findGe(this.comparator,entrySupplier, key);
        if (ite.isEq()) {
            long eqkey = ite.peek()._getKey();
            Iterable<T> it = () -> new LongBTree.Iterator(ite.getNode(), ite.getIndex(),entrySupplier);
            return StreamSupport.stream(it.spliterator(), false).filter(e -> e._getKey() != eqkey).iterator();
        } else {
            Iterable<T> it = () -> new LongBTree.Iterator(ite.getNode(), ite.getIndex(),entrySupplier);
            return StreamSupport.stream(it.spliterator(), false).iterator();
        }

    }

    @Override
    public <T extends Entry> java.util.Iterator<T> findLE(long key) {
        LeafNode.Iterator ite = this.rootNode.findLe(this.comparator, entrySupplier,key);
        Iterable<T> it = () -> new LongBTree.Iterator(ite.getNode(), ite.getIndex(),entrySupplier).reverse(true);
        return StreamSupport.stream(it.spliterator(), false).iterator();
    }

    @Override
    public <T extends Entry> java.util.Iterator<T> findLT(long key) {
        LeafNode.Iterator ite = this.rootNode.findLe(this.comparator,entrySupplier, key);
        if (ite.isEq()) {
            long eqKey = ite.peek()._getKey();
            Iterable<T> it = () -> new LongBTree.Iterator(ite.getNode(), ite.getIndex(),entrySupplier).reverse(true);
            return StreamSupport.stream(it.spliterator(), false).filter(e -> e._getKey() != eqKey).iterator();
        } else {
            Iterable<T> it = () -> new LongBTree.Iterator(ite.getNode(), ite.getIndex(),entrySupplier).reverse(true);
            return StreamSupport.stream(it.spliterator(), false).iterator();
        }

    }

    @Override
    public <T extends Entry> java.util.Iterator<T> all(boolean asc) {
        return new LongBTree.Iterator(
                new ReadOnlyLeafNode(mainBuffer, pageSize * indexNodeCount, maxItemPerNode, pageSize), 0,entrySupplier);
    }

    public LongBTree.LeafIterator leafIterator(LeafNode leafNode) {
        return new LongBTree.LeafIterator(leafNode);
    }

    public LongBTree.NodeIterator nodeIterator(ReadOnlyNode indexNode) {
        return new LongBTree.NodeIterator(indexNode);
    }

    @Override
    public <T extends Entry> java.util.Iterator<T> findEQ(long key) {
        LeafNode.Iterator ite = this.rootNode.findGe(this.comparator,entrySupplier, key);
        if (ite.isEq()) {
            Iterable<T> it = () -> new LongBTree.EqIterator(ite.getNode(), ite.getIndex(), ite.peek()._getKey(),entrySupplier);
            return StreamSupport.stream(it.spliterator(), false).iterator();
        } else {
            return LeafNode.Iterator.Empty;
        }

    }

    public static class Iterator<T extends Entry> implements java.util.Iterator<T> {

        private LeafNode leafNode;
        protected LeafNode.Iterator<T> ite;
        private boolean reverse;
        private Supplier<T> entrySupplier;

        public Iterator(LeafNode leafNode, int index, Supplier<T> entrySupplier) {
            this.entrySupplier = entrySupplier;
            this.leafNode = leafNode;
            ite = this.leafNode.iterator(index, entrySupplier);
            if (!ite.hasNext() && this.leafNode.hasNext()) {
                this.leafNode = this.leafNode.next();
                this.ite = this.leafNode.iterator(0, entrySupplier);
            }
            this.reverse = false;
        }

        @Override
        public boolean hasNext() {
            return ite.hasNext();
        }

        @Override
        public T next() {
            if (!reverse) {
                return forward();
            } else {
                return backward();
            }
        }

        private T forward() {
            T e = this.ite.next();
            if (!this.ite.hasNext() && this.leafNode.hasNext()) {
                this.leafNode = this.leafNode.next();
                this.ite = this.leafNode.iterator(0,this.entrySupplier);
            }
            return (T) e;
        }

        private T backward() {
            T e = this.ite.next();
            if (!this.ite.hasNext() && this.leafNode.hasPrevious()) {
                this.leafNode = this.leafNode.previous();
                this.ite = this.leafNode.iterator(this.leafNode.getSize() - 1,this.entrySupplier).reverse(true);
            }
            return e;
        }

        public Iterator reverse(boolean reverse) {
            this.reverse = reverse;
            this.ite.reverse(reverse);
            if (reverse) {
                if (!ite.hasNext() && this.leafNode.hasPrevious()) {
                    this.leafNode = this.leafNode.previous();
                    this.ite = this.leafNode.iterator(this.leafNode.getSize() - 1,this.entrySupplier).reverse(reverse);
                }
            }

            return this;
        }
    }

    public static final class EqIterator<T extends Entry> extends Iterator<T> {

        private long key;

        public EqIterator(LeafNode leafNode, int index, long key,Supplier<T> entrySupplier) {
            super(leafNode, index,entrySupplier);
            this.key = key;
        }

        @Override
        public boolean hasNext() {
            return ite.hasNext() && checkEqual();
        }

        private boolean checkEqual() {
            return this.ite.peek()._getKey() == key;
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
