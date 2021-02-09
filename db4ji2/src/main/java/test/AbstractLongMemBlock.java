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
package test;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.function.Supplier;

/**
 * @author nuwansa
 */
public abstract class AbstractLongMemBlock implements NumberQueryBlock{

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

    @Override
    public Iterator all(Iterator ite) {
        int size = getSize();
        if (size == 0) {
            return Iterator.EMPTY;
        }
        return ite.init(this,0,getSize());
    }
    
    protected Iterator findGE(Iterator ite,long key) {
        int size = getSize();
        int pos = binarySearch(0, size, key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,pos,size);
        } else {
            pos = adjustEqLowPos(pos, key);
            return ite.init(this,pos, getSize());
        }
    }

    protected Iterator findGT(Iterator ite,long key) {
        int size = getSize();
        int pos = binarySearch(0, size, key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,pos,size);
        } else {
            pos = adjustEqUpperPos(pos, key);
            return ite.init(this,pos, getSize());
        }
    }

    protected Iterator findLE(Iterator ite,long key) {
        int size = getSize();
        int pos = binarySearch(0, getSize(), key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,0,pos);
        } else {
            pos = adjustEqUpperPos(pos, key);
            return ite.init(this,0, pos);
        }

    }

    protected Iterator findLT(Iterator ite,long key) {
        int size = getSize();
        int pos = binarySearch(0, getSize(), key);
        if (pos < 0) {
            pos = -pos - 1;
            return ite.init(this,0,pos);
        } else {
            pos = adjustEqLowPos(pos, key);
            return ite.init(this,0, pos);
        }
    }

    protected Iterator findEQ(Iterator ite,long key) {
        int size = getSize();
        int pos = binarySearch(0, getSize(), key);
        if (pos >= 0) {
            pos = adjustEqLowPos(pos, key);
            return ite.init(this,pos, size).withEqKey(key);
        } else {
            return Iterator.EMPTY;
        }
    }
    
    private int adjustEqLowPos(int pos, long matchKey) {
        while (pos >= 0 && getKey(pos) == matchKey) {
            pos--;
        }
        return pos + 1;
    }

    private int adjustEqUpperPos(int pos, long matchKey) {
        while (pos < this.size && getKey(pos) == matchKey) {
            pos++;
        }
        return pos;
    }
    
    protected long getKey(int pos)
    {
        return this.longBuffer.get(pos);
    }
    
    @Override
    public long getValue(int pos)
    {
        return this.longBuffer.get(this.maxItemCount + pos);
    }
    
    public void updateSize(int size) {
        long val = this.longBuffer.get(this.longBuffer.limit() - 1);
        val = (val & 0xFFFFFFFF00000000L) | size & 0xFFFFFFFFL;
        this.longBuffer.put(this.longBuffer.limit() - 1, val);
        this.size = size;
    }

    @Override
    public int getSize() {
        if (this.size == -1) {
            this.size = (int) this.longBuffer.get(this.longBuffer.limit() - 1);
        }
        return size;
    }

    public LongEntry entry(int index, Supplier<LongEntry> sup) {
        return sup.get().with(this.longBuffer.get(index), this.longBuffer.get(maxItemCount + index));
    }

    protected int binarySearch(int fromIndex, int toIndex, long key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = getKey(mid);
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
}
