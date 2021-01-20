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
package com.cloudimpl.db4ji2.idx.str;

import com.cloudimpl.db4ji2.idx.str.StringQueryable;
import com.cloudimpl.db4ji2.idx.str.XCharSequence;
import com.cloudimpl.db4ji2.idx.str.XBasicString;
import com.cloudimpl.db4ji2.idx.str.StringBlock;
import com.cloudimpl.db4ji2.core.AbstractLongMemBlock;
import com.cloudimpl.db4ji2.core.LongEntry;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * @author nuwansa
 */
public class StringMemBlock extends AbstractLongMemBlock implements StringQueryable {

    private static ThreadLocal<XBasicString> thrLocal = ThreadLocal.withInitial(() -> new XBasicString());
   
    private final StringBlock stringBlock;
    public StringMemBlock(ByteBuffer byteBuf,StringBlock stringBlock, int offset, int pageSize) {
        super(byteBuf, offset, pageSize);
        this.stringBlock = stringBlock;
        setComparator(this::compare);
        
    }

    public boolean put(LongBuffer temp, CharSequence key, long value) {
        thrLocal.get().init(key);
        int size = getSize();
        if (size == maxItemCount) {
            return false;
        }
        if (size == 0) {
            long longKey = stringBlock.append(key);
            this.longBuffer.put(0, longKey);
            this.longBuffer.put(maxItemCount, value);
            this.min = longKey;
            this.max = longKey;
        } else {
            //      long[] arr = this.longBuffer.array();
            long longKey;
            int pos = binarySearch(0, size, -1000);
            if (pos <= 0) {
                pos = -pos - 1;
                longKey = stringBlock.append(key);
            }
            else
            {
                longKey = longBuffer.get(pos);
            }
            temp = temp.limit(offset + size).position(offset + pos);
            this.longBuffer.position(pos + 1);
            //  System.out.println("temp:" + temp.remaining());
            // temp.flip();
            this.longBuffer.put(temp);
            temp = temp.limit(offset + maxItemCount + size).position(offset + pos + maxItemCount);
            this.longBuffer.position(pos + maxItemCount + 1);
            // temp.flip();
            this.longBuffer.put(temp);

            //  System.arraycopy(arr, pos, arr, pos + 1, size - pos);
            // System.arraycopy(arr, pos + maxItemCount, arr, maxItemCount + pos + 1, size - pos);
            this.longBuffer.put(pos, longKey);
            this.longBuffer.put(maxItemCount + pos, value);
            this.min = compare(min, longKey) > 0? longKey:this.min;
            this.max = compare(max, longKey) < 0?longKey:this.max;
        }
        size++;
        updateSize(size);
        this.size = size;
        return true;
    }
    
    @Override
    public java.util.Iterator<LongEntry> all(boolean asc) {
        return all(asc, () -> new LongEntry());
    }

    @Override
    public java.util.Iterator<LongEntry> findGE(CharSequence key) {
        thrLocal.get().init(key);
        return findGE(-1000, () -> new LongEntry());
    }

    @Override
    public java.util.Iterator<LongEntry> findGT(CharSequence key) {
        thrLocal.get().init(key);
        return findGT(-1000, () -> new LongEntry());
    }

    @Override
    public java.util.Iterator<LongEntry> findLE(CharSequence key) {
        thrLocal.get().init(key);
        return findLE(-1000, () -> new LongEntry());
    }

    @Override
    public java.util.Iterator<LongEntry> findLT(CharSequence key) {
        thrLocal.get().init(key);
        return findLT(-1000, () -> new LongEntry());
    }

    @Override
    public java.util.Iterator<LongEntry> findEQ(CharSequence key) {
        thrLocal.get().init(key);
        return findEQ(-1000, () -> new LongEntry());
    }


    @Override
    public void updateSize(int size) {
        super.updateSize(size);
    }

    @Override
    public int getSize() {
       return super.getSize();
    }

    public int compare(long l, long r) {
        XCharSequence left;
        XCharSequence right;
        if (l < 0) {
            left = thrLocal.get();
            l = 0;
        } else {
            left = stringBlock;
        }

        if (r < 0) {
            right = thrLocal.get();
            r = 0;
        } else {
            right = stringBlock;
        }

//        int ret =  left.toString().compareTo(right.toString());
//       System.out.println("l :"+left+ " r : "+right + "ret:"+ret);
//       return ret;
        char lc = left.nextChar(l++);
        char rc = right.nextChar(r++);
        while (lc != StringBlock.NULL && rc != StringBlock.NULL) {
            if (lc != rc) {
                return lc - rc;
            }
            lc = left.nextChar(l++);
            rc = right.nextChar(r++);
        }

        if (lc != StringBlock.NULL && rc == StringBlock.NULL) {
            System.out.println("l:" + left + "> r: " + right);
            return lc;
        } else if (lc == StringBlock.NULL && rc != StringBlock.NULL) {
            System.out.println("l:" + left + "< r: " + right);
            return -rc;
        } else if (lc == StringBlock.NULL && rc == StringBlock.NULL) {
            //     System.out.println("l:"+left+ "= r: "+right);
            return 0;
        }
        throw new RuntimeException("unknown size");
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
