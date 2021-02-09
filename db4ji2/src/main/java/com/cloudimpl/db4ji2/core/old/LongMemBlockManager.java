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
package com.cloudimpl.db4ji2.core.old;

import com.cloudimpl.db4ji2.idx.lng.old.LongQueryBlockAggregator;
import com.cloudimpl.db4ji2.idx.lng.old.LongQueryable;
import com.cloudimpl.db4ji2.core.old.LongMemBlock;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author nuwansa
 */
public class LongMemBlockManager extends LongQueryBlockAggregator {

    private final int memSize;
    private final int pageSize;
    private final ByteBuffer mainBuf;
    private final LongBuffer temp;
    private LongMemBlock memBlock;
    private final int pageCapacity;
    private int offset;
    private final LongMemBlock[] blocks;
    private int currentBlkIndex;
    private LongComparable comparable;

    public LongMemBlockManager(int memSize, int pageSize, LongComparable comparable) {
        this.comparable = comparable;
        this.memSize = memSize;
        this.pageSize = pageSize;
        this.pageCapacity = this.pageSize / 8;
        this.currentBlkIndex = 0;
        this.blocks = new LongMemBlock[memSize / pageSize];
        mainBuf = ByteBuffer.allocateDirect(memSize);
        temp = mainBuf.asLongBuffer();
        this.offset = 0;
        memBlock = new LongMemBlock(mainBuf, this.offset, pageSize);
        blocks[this.currentBlkIndex++] = memBlock;
    }

    public boolean put(long key, long value) {
        // temp.position( this.currentBlkIndex * pageCapacity);
        if (!memBlock.put(temp, key, value)) {
            this.offset += pageSize;
            //     temp.clear();
            memBlock = new LongMemBlock(mainBuf, this.offset, pageSize);
            blocks[this.currentBlkIndex++] = memBlock;
            memBlock.put(temp, key, value);
        }
        return true;
    }

    @Override
    protected Stream<LongQueryable> getBlockStream() {
        return IntStream.range(0, currentBlkIndex).mapToObj(i -> blocks[i]);
    }

    public int getBlockCount() {
        return this.currentBlkIndex;
    }

    public LongMemBlock getBlock(int index) {
        return this.blocks[index];
    }

    public static void main(String[] args) {
        final List<Integer> list1
                = Arrays.asList(IntStream.range(0, 10_000_000).boxed().toArray(Integer[]::new));
        // List list2 = Arrays.asList(IntStream.range(256, 511).boxed().toArray());
        // List<Integer> list3 = new LinkedList<>(list1);
        List<Integer> list4 = new LinkedList<>(list1);
        // Collections.shuffle(list3);
        Collections.shuffle(list4);

        LongMemBlockManager man = new LongMemBlockManager(4096 * 39216, 4096,Long::compare);
        // list3.forEach(i->man.put(i, i * 10));
        System.gc();
        long start = System.nanoTime();
        list4.forEach(i -> man.put(i, i * 10));
        long end = System.nanoTime();
        double d = ((double) (end - start)) / list1.size();
        System.out.println("op/s" + d + "thro:" + (1000000000 / d));
        AtomicInteger i = new AtomicInteger(list1.size() - 1);
        start = System.currentTimeMillis();
        man.all(false)
                .forEachRemaining(
                        e -> {
                            if (e.getKey() != i.get() && e.getValue() != i.get() * 10) {
                                throw new RuntimeException("error  :" + e.getKey() + ":" + i.get());
                            }
                            i.decrementAndGet();
                        });
        end = System.currentTimeMillis();
        System.out.println("diff :" + (end - start));
        System.out.println("i:" + i.get());
        System.out.println(man.getBlock(0));
        System.out.println(man.getBlock(1));
    }
}
