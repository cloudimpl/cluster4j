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

import com.cloudimpl.db4ji2.core.LongComparable;
import com.cloudimpl.db4ji2.core.MergeItem;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author nuwansa
 */
public class StringColumnIndex extends StringQueryBlockAggregator {

    private final String columnName;
    private final StringMemBlockPool memPool;
    private final StringCompactionManager compactMan;
    private StringMemBlock currentBlock;
    private final LongComparable comparator;
    private final Supplier<StringEntry> entrySupplier;
    private final CopyOnWriteArrayList<StringQueryable> queryables = new CopyOnWriteArrayList<>();

    public StringColumnIndex(String colName, int memSize, int pageSize, LongComparable comparator,Supplier<StringEntry> entrySupplier) {
        this.columnName = colName;
        this.comparator = comparator;
        this.entrySupplier = entrySupplier;
        memPool = new StringMemBlockPool(memSize, pageSize);
        compactMan = new StringCompactionManager(this);
        compactMan.init(7);
        currentBlock = memPool.aquire();
        add(currentBlock);
    }

    public void put(CharSequence key, long value) {
        boolean ok = currentBlock.put(memPool.getLongBuffer(), key, value);
        if (!ok) {
            compactMan.submit(new MergeItem<>(0, currentBlock));
            currentBlock = memPool.aquire();
            if (currentBlock == null) {
                // throw new Db4jException
                System.out.println("columnIndex :" + columnName + " not enough blocks available in pool");
                try {
                    System.gc();
                    Thread.sleep(1000);
                    currentBlock = memPool.aquire();
                } catch (InterruptedException ex) {
                    Logger.getLogger(StringColumnIndex.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            add(currentBlock);
            currentBlock.put(memPool.getLongBuffer(), key, value);
        }
    }

    protected synchronized void remove(StringQueryable query) {
        queryables.remove(query);
    }

    public int getQueryBlockCount() {
        return queryables.size();
    }

    public LongComparable getComparator() {
        return comparator;
    }

    public Supplier<StringEntry> getEntrySupplier() {
        return entrySupplier;
    }

    protected synchronized void update(List<? extends StringQueryable> removables, StringQueryable update) {
        queryables.removeAll(removables);
        queryables.add(update);
    }

    @Override
    protected synchronized Stream<StringQueryable> getBlockStream() {
        return queryables.stream();
    }

    protected synchronized void add(StringQueryable query) {
        queryables.add(query);
    }

    protected void release(StringQueryable queryable) {
        if (queryable instanceof StringMemBlock) {
            memPool.release((StringMemBlock) queryable);
        }
        //    else if (queryable instanceof BTree) {
        //      BTree.class.cast(queryable).close();
        //    }
    }

    public void dumpStats() {
        queryables.stream()
                .collect(Collectors.groupingBy(c -> c.getSize()))
                .forEach((i, list) -> System.out.println("k:" + i + "->" + list.size()));
        ;
    }

//    public static void main(String[] args) throws InterruptedException {
//        StringColumnIndex idx = new StringColumnIndex("Test", 4096 * 1280 * 32, 4096, Long::compare,()->new LongEntry());
//        // List<Integer> list1 =
//        //   Arrays.asList(IntStream.range(0, 20_000_000).boxed().toArray(Integer[]::new));
//        //  Collections.shuffle(list1);
//
//        Random r = new Random(System.currentTimeMillis());
//        long i = 0;
//        long size = 40_000_000;
//        int rate = 0;
//        long start = System.currentTimeMillis();
//        long lasKey = 0;
//        long firstKey = 0;
//        RateLimiter limiter = RateLimiter.create(400000);
//        while (i < size) {
//            //  limiter.acquire();
//            lasKey = r.nextInt();
//            if (firstKey == 0) {
//                firstKey = lasKey;
//            }
//            idx.put(lasKey, i);
//            if (i % 1000000 == 0) {
//                System.out.println("query : " + idx.getQueryBlockCount());
//                System.gc();
//            }
//            rate++;
//            long end = System.currentTimeMillis();
//            if (end - start >= 1000) {
//                System.out.println(
//                        "rate-----------------------------------: "
//                        + rate
//                        + " : "
//                        + idx.getQueryBlockCount()
//                        + " current : "
//                        + (i));
//                rate = 0;
//                start = System.currentTimeMillis();
//            }
//            i++;
//        }
//        long vlk = lasKey;
//        // list1.forEach(i -> idx.put(i, i * 10));
//        System.out.println("completed:---------------------------------------------");
//
//        // int size = list1.size();
//        System.out.println("laskKey: " + vlk);
//        StreamSupport.stream(((Iterable<LongEntry>) () -> idx.findEQ((long) (vlk))).spliterator(), false)
//                .forEach(System.out::println);
//        start = System.currentTimeMillis();
//        System.out.println(
//                StreamSupport.stream(((Iterable<LongEntry>) () -> idx.all(true)).spliterator(), false).count());
//        long end = System.currentTimeMillis();
//        System.out.println("time : " + (end - start));
//        // list1 = null;
//        Thread.sleep(10000);
//        System.out.println(
//                StreamSupport.stream(((Iterable<LongEntry>) () -> idx.all(true)).spliterator(), false).count());
//        start = System.currentTimeMillis();
//        idx.findGE((long) (size - 1)).forEachRemaining(t -> {
//        });
//        end = System.currentTimeMillis();
//        System.out.println("diff: " + (end - start));
//        start = System.currentTimeMillis();
//        idx.findEQ((long) (firstKey))
//                .forEachRemaining(
//                        t -> {
//                            System.out.println("|" + t);
//                        });
//        end = System.currentTimeMillis();
//        System.out.println("diff: " + (end - start));
//        System.out.println("xxx: " + idx.getQueryBlockCount());
//        idx.dumpStats();
//        //    idx.all(true)
//        //        .forEachRemaining(
//        //            entry -> {
//        //              System.out.println("e: " + entry);
//        //            });
//        Thread.sleep(100000000);
//    }
}
