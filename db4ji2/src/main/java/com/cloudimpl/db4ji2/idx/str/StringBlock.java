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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;

/**
 *
 * @author nuwan
 */
public class StringBlock implements XCharSequence {

    public static final char NULL = 0x00;
    private final Function<Integer, ByteBuffer> bufferProvider;
    private final ArrayList<StringPage> pages;
    private StringPage current;
    private int currentIndex;
    private int pageSize;

    public StringBlock(int pageSize, Function<Integer, ByteBuffer> bufferProvider) {
        this.pageSize = pageSize;
        this.bufferProvider = bufferProvider;
        this.pages = new ArrayList<>();
        this.current = null;
        this.currentIndex = -1;
    }

    public long append(CharSequence value) {
        if (value.isEmpty()) {
            int pageIndex = append(value.charAt(0));
            int offset = this.current.getPosition() - 1;
            append((char) 0x00);
            return ((long) pageIndex) << 32 | offset;
        }
        int pageIndex = append(value.charAt(0));
        int offset = this.current.getPosition() - 1;
        int i = 1;
        while (i < value.length()) {
            append(value.charAt(i));
            i++;
        }
        append(NULL);
        return ((long) pageIndex) << 32 | offset;
    }

    public long append(long pos, XCharSequence value) {
        char c = value.nextChar(pos++);
        int pageIndex = append(c);
        int offset = this.current.getPosition() - 1;
        if (c == NULL) {
            return ((long) pageIndex) << 32 | offset;
        }

        while (c != NULL) {
            c = value.nextChar(pos++);
        }
        append(NULL);
        return ((long) pageIndex) << 32 | offset;
    }

    @Override
    public char nextChar(long pos) {
        int page = (int) (pos >> 32 & 0xFFFFFFFF);
        int index = (int) pos;
        return this.pages.get(page).getChar(index);
    }

    private int append(char c) {
        if (this.current == null || this.current.isFull()) {
            this.current = new StringPage(bufferProvider.apply(this.pageSize));
            this.pages.add(current);
            this.currentIndex++;
        }
        this.current.append(c);
        return this.currentIndex;
    }

    public String toString(long pos) {
        StringBuilder builder = new StringBuilder();
        char c;
        while ((c = nextChar(pos++)) != NULL) {
            builder.append(c);
        }
        return builder.toString();
    }
}
