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
import com.cloudimpl.db4ji2.core.LongComparable;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.function.Supplier;

/**
 * @author nuwansa
 */
public abstract class AbstractLongMemBlock {

    protected final LongBuffer longBuffer;
    protected final int maxItemCount;
    protected final int offset;
    protected long max;
    protected long min;
    protected int size = -1;
    protected LongComparable comparable;

    public AbstractLongMemBlock(ByteBuffer byteBuf, int offset, int pageSize) {
        this.longBuffer = byteBuf.position(offset).asLongBuffer().limit(pageSize / 8);
        this.offset = offset / 8;
        this.maxItemCount = ((pageSize / 8) - 1) / 2;
    }

    protected final void setComparator(LongComparable comp) {
        this.comparable = comp;
    }

    protected Iterator all(boolean asc, Supplier<LongEntry> sup) {
        int size = getSize();
        if (size == 0) {
            return Iterator.EMPTY;
        }
        Iterator ite;
        if (asc) {
            ite = iterator(0, sup);
        } else {
            ite = iterator(size - 1, sup).reverse();
        }
        return ite;
    }

    protected java.util.Iterator<LongEntry> all(boolean asc) {
        return all(asc, () -> new LongEntry());
    }

    protected java.util.Iterator<LongEntry> findGE(long key) {
        return findGE(key, () -> new LongEntry());
    }

    protected Iterator findGE(long key, Supplier<LongEntry> sup) {
        int size = getSize();
        if (size == 0 || this.comparable.compare(max, key) < 0) {
            return Iterator.EMPTY;
        }
        int pos = binarySearch(0, size, key);
        if (pos < 0) {
            pos = -pos - 1;
            return iterator(pos, sup);
        } else {
            return iterator(pos, sup).hasEq();
        }

    }

    protected java.util.Iterator<LongEntry> findGT(long key) {
        return findGT(key, () -> new LongEntry());
    }

    protected java.util.Iterator<LongEntry> findGT(long key, Supplier<LongEntry> sup) {
        return findGE(key, sup).skipEq();
    }

    protected java.util.Iterator<LongEntry> findLE(long key) {
        return findLE(key, () -> new LongEntry());
    }

    protected Iterator findLE(long key, Supplier<LongEntry> sup) {
        int size = getSize();
        if (size == 0 || this.comparable.compare(min, key) > 0) {
            return Iterator.EMPTY;
        }
        int pos = binarySearch(0, getSize(), key);
        if (pos < 0) {
            pos = -pos - 2;
            return iterator(pos, sup).reverse();
        } else {
            return iterator(pos, sup).reverse().hasEq();
        }

    }

    protected java.util.Iterator<LongEntry> findLT(long key) {
        return findLT(key, () -> new LongEntry());
    }

    protected java.util.Iterator<LongEntry> findLT(long key, Supplier<LongEntry> sup) {
        return findLE(key, sup).skipEq();
    }

    protected java.util.Iterator<LongEntry> findEQ(long key) {
        return findEQ(key, () -> new LongEntry());
    }

    protected java.util.Iterator<LongEntry> findEQ(long key, Supplier<LongEntry> sup) {
        int size = getSize();
        if (size == 0 || comparable.compare(min, key) > 0 || comparable.compare(max, key) < 0) {
            return Iterator.EMPTY;
        }
        int pos = binarySearch(0, getSize(), key);
        if (pos >= 0) {
            return new EqIterator(pos, this, sup, this.longBuffer.get(pos));
        } else {
            return Iterator.EMPTY;
        }
    }

    public void updateSize(int size) {
        long val = this.longBuffer.get(this.longBuffer.limit() - 1);
        val = (val & 0xFFFFFFFF00000000L) | size & 0xFFFFFFFFL;
        this.longBuffer.put(this.longBuffer.limit() - 1, val);
        this.size = size;
    }

    protected int getSize() {
        if (this.size == -1) {
            this.size = (int) this.longBuffer.get(this.longBuffer.limit() - 1);
        }
        return size;
    }

    public LongEntry entry(int index, Supplier<LongEntry> sup) {
        return sup.get().set(this.longBuffer.get(index), this.longBuffer.get(maxItemCount + index));
    }

    public Iterator iterator(int index, Supplier<LongEntry> sup) {
        return new Iterator(index, this, sup);
    }

    protected int binarySearch(int fromIndex, int toIndex, long key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = this.longBuffer.get(mid);
            int ret = comparable.compare(midVal, key);
            if (ret < 0) {
                low = mid + 1;
            } else if (ret > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1); // key not found.
    }

    @Override
    public String toString() {
        String s = "";
        int i = 0;
        while (i < getSize()) {
            s += this.longBuffer.get(i);
            s += ",";
            i++;
        }
        s += "[";
        i = 0;
        while (i < getSize()) {
            s += this.longBuffer.get(this.maxItemCount + i);
            s += ",";
            i++;
        }
        s += "]";
        return s;
    }

    public static class Iterator implements java.util.Iterator<LongEntry> {

        protected int index;
        protected final AbstractLongMemBlock block;
        protected Supplier<LongEntry> supplier;
        protected boolean reverse = false;
        public static final Iterator EMPTY = new Iterator(0, null, null);
        private boolean skipEq;
        private long eqKey;
        private boolean hasEq = false;

        public Iterator(int index, AbstractLongMemBlock block, Supplier<LongEntry> supplier) {
            this.index = index;
            this.block = block;
            this.supplier = supplier;
        }

        public Iterator hasEq() {
            this.hasEq = true;
            return this;
        }

        public Iterator skipEq() {
            if (this.hasEq) {
                this.skipEq = true;
                this.eqKey = peek().getKey();
            }
            return this;
        }

        public void reset(int index) {
            this.index = index;
        }

        public Iterator reverse() {
            this.reverse = !this.reverse;
            return this;
        }

        @Override
        public boolean hasNext() {
            return this.block != null && doSkipEq();
        }

        private boolean checkNext() {
            if (reverse) {
                return this.index >= 0;
            } else {
                return index < this.block.getSize();
            }
        }

        private boolean doSkipEq() {
            if (this.skipEq) {
                while (checkNext() && peek().getKey() == this.eqKey) {
                    next();
                }
            }
            return checkNext();
        }

        @Override
        public LongEntry next() {
            LongEntry e = this.block.entry(index, this.supplier);
            if (reverse) {
                index--;
            } else {
                index++;
            }
            return e;
        }

        public LongEntry peek() {
            return this.block.entry(index, this.supplier);
        }
    }

    public static final class EqIterator extends Iterator {

        private final long key;

        public EqIterator(int index, AbstractLongMemBlock block, Supplier<LongEntry> supplier, long key) {
            super(index, block, supplier);
            this.key = key;
        }

        @Override
        public boolean hasNext() {
            return super.hasNext() && checkEqual();
        }

        private boolean checkEqual() {
            LongEntry e = this.block.entry(index, this.supplier);
            return e.getKey() == key;
        }
    }
}
