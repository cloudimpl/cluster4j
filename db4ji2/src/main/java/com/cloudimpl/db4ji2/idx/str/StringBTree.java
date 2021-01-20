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

import com.cloudimpl.db4ji2.idx.str.StringBlock;
import com.cloudimpl.db4ji2.core.LongBTree;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 * @author nuwan
 */
public class StringBTree implements StringQueryable {

    private final StringBlock stringBlock;
    private final LongBTree tree;
    private CharSequence lastKey;
    private StringEntry lastEntry;
    private long lastKeyIndex = -1;
    private static final ThreadLocal<XBasicString> thrLocal = ThreadLocal.withInitial(() -> new XBasicString());
    private Supplier<StringEntry> entrySupplier;

    public StringBTree(int maxItemCount,
            int pageSize,
            LongBTree.Header header,
            Function<Integer, ByteBuffer> bufferProvider, int stringPageSize, Function<Integer, ByteBuffer> stringBufferProvider, Supplier<StringEntry> entrySupplier) {
        this.entrySupplier = entrySupplier;
        this.stringBlock = new StringBlock(stringPageSize, stringBufferProvider);
        this.tree = new LongBTree(maxItemCount, pageSize, header, bufferProvider, this::compare, this::entrySupplier);
    }

    protected void init() {
        this.tree.init();
    }

    public boolean put(CharSequence key, long value) {
        if (lastKey == null || CharSequence.compare(key, lastKey) != 0) {
            lastKeyIndex = stringBlock.append(key);
            this.lastKey = key;
        }
        return this.tree.put(lastKeyIndex, value);
    }

    protected boolean put(StringEntry entry) {
        if (lastEntry == null || StringEntry.compare(entry, lastEntry) != 0) {
            lastKeyIndex = stringBlock.append(entry._getKey(), entry.getStringBlock());
            //     System.out.println("key: "+key + " ->"+new XString().init(stringBlock, lastKeyIndex));
            this.lastEntry = entry;
        }
        return this.tree.put(lastKeyIndex, entry.getValue());
    }

    public StringBlock getStringBlock() {
        return stringBlock;
    }

    public LongBTree getTree() {
        return tree;
    }

    public void complete() {
        this.tree.complete();
    }

    private StringEntry entrySupplier() {
        return this.entrySupplier.get().setBlock(stringBlock);
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
        return XCharSequence.compare(l, left, r, right);
    }

    public static StringBTree create(int maxItemCount, int pageSize, int stringPageSize, Supplier<StringEntry> entrySupplier) {
        StringBTree tree
                = new StringBTree(
                        maxItemCount,
                        pageSize,
                        new LongBTree.Header(ByteBuffer.allocateDirect(1024)),
                        size -> ByteBuffer.allocateDirect(size), stringPageSize, size -> ByteBuffer.allocateDirect(size), entrySupplier);
        tree.init();
        return tree;
    }

    @Override
    public Iterator<StringEntry> all(boolean asc) {
        return tree.all(asc);
    }

    @Override
    public Iterator<StringEntry> findEQ(CharSequence key) {
        thrLocal.get().init(key);
        return tree.findEQ(-1000);
    }

    @Override
    public Iterator<StringEntry> findGE(CharSequence key) {
        thrLocal.get().init(key);
        return tree.findGE(-1000);
    }

    @Override
    public Iterator<StringEntry> findGT(CharSequence key) {
        thrLocal.get().init(key);
        return tree.findGT(-1000);
    }

    @Override
    public Iterator<StringEntry> findLE(CharSequence key) {
        thrLocal.get().init(key);
        return tree.findLE(-1000);
    }

    @Override
    public Iterator<StringEntry> findLT(CharSequence key) {
        thrLocal.get().init(key);
        return tree.findLT(-1000);
    }

    @Override
    public int getSize() {
        return this.tree.getSize();
    }

    public static String randomString() {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 20;
        Random random = new Random();

        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        return generatedString;
    }

    public static void main(String[] args) throws InterruptedException {
        StringBTree tree = StringBTree.create(4 * 1024 * 1024, 4096, 4096, () -> new StringEntry());

        String l = "vgC063VOenHDpyZqdSJX";
        String right = "zzzm7M8WO6miwWOaWgxT";

        int ret = l.compareTo(right);

        byte[] b = {'a', 'b', 'c', (char) 0x00, 'd', 'e'};
        String s = new String(b);
        System.out.println(s);
        Random r = new Random(System.currentTimeMillis());
        String[] arr = new String[4 * 1024 * 1024];

        int i = 0;
        while (i < 4 * 1024 * 1024) {
            arr[i] = randomString();
            i++;
        }
        arr = Arrays.asList(arr).stream().sorted(String::compareTo).toArray(String[]::new);
        i = 0;
        System.gc();
        long start = System.currentTimeMillis();
        while (i < 4 * 1024 * 1024) {
            tree.put(arr[i], i);
            i++;
        }
        tree.complete();
        long end = System.currentTimeMillis();
        System.out.println("ops:" + ((double) i / (end - start)) * 1000);

        //tree.getTree().all(true).forEachRemaining(e -> System.out.println(a.init(tree.getStringBlock(), e.getKey())));
        System.out.println("searching:" + arr[4 * 1024 * 1024 - 10]);
        while (true) {
            start = System.nanoTime();
            tree.findEQ(arr[4 * 1024 * 1024 - 10]).forEachRemaining(e -> System.out.println(tree.getStringBlock().toString(e.getKey()) + ":" + e.getValue()));
            end = System.nanoTime();
            System.out.println("time:" + (end - start));
            //break;
        }

        //  Thread.sleep(100000000);
    }
}
