/*
 * Copyright 2021 nuwan.
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

import com.cloudimpl.db4ji2.core.old.Validation;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.agrona.BitUtil;

/**
 *
 * @author nuwan
 */
public class DirectStringBlock implements StringBlock {

    private final ArrayList<StringPage> pages;
    private StringPage current;
    private int pageSize;
    private long currentPos;
    private int exp;

    public DirectStringBlock(int pageSize) {
        Validation.checkCondition(BitUtil.isPowerOfTwo(pageSize), () -> "StringBlock page size should be power of two");
        this.exp = Math.getExponent(pageSize);
        this.pageSize = pageSize;
        this.pages = new ArrayList<>();
        this.current = null;
        this.currentPos = 0;
    }

    @Override
    public long append(CharSequence value) {
        long temp = this.currentPos;
        int i = 0;
        while (i < value.length()) {
            append(value.charAt(i));
            i++;
        }
        append(NULL);
        return temp;
    }

    @Override
    public long append(long pos, XCharSequence value) {
        long temp = this.currentPos;
        char c;
        do {
            c = value.nextChar(pos++);
            append(c);
        } while (c != NULL);
        return temp;
    }

    @Override
    public char nextChar(long pos) {
        try {
            int page = (int) (pos >> exp);
            int index = (int) (pos & (this.pageSize - 1));
            return this.pages.get(page).getChar(index);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    private void append(char c) {
        if (this.current == null || this.current.isFull()) {
            this.current = new StringPage(ByteBuffer.allocateDirect(this.pageSize));
            this.pages.add(current);
        }
        this.current.append(c);
        this.currentPos++;
    }

    @Override
    public String toString(long pos) {
        StringBuilder builder = new StringBuilder();
        char c;
        while ((c = nextChar(pos++)) != NULL) {
            builder.append(c);
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        int shift = Math.getExponent(4096);
        long l = 4096;
        System.out.println("shift:" + shift);
        System.out.println(l >> shift);
        System.out.println(l & 4096 - 1);
    }

    public static class StringPage {

        private ByteBuffer buf;

        public StringPage(ByteBuffer buf) {
            this.buf = buf;
        }

        public int getPosition() {
            return this.buf.position();
        }

        public int getCapacity() {
            return this.buf.capacity();
        }

        public void append(char c) {
            this.buf.put((byte) c);
        }

        public char getChar(int index) {
            return (char) this.buf.get(index);
        }

        public boolean isFull() {
            return !this.buf.hasRemaining();
        }
    }
}
