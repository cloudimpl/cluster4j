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
package test;

import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;
import org.green.jelly.JsonNumber;
import org.green.jelly.MutableJsonNumber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 *
 * @author nuwan
 */
public class NumberColumnIndex implements ColumnIndex {

    private final String name;
    private final NumberMemBlockPool memBlockPool;
    private Number2MemBlock currentBlock;
    private final CompactionManager compactionManager;

    public NumberColumnIndex(String name, int memPoolSize) {
        this.name = name;
        compactionManager = new CompactionManager(6, this, this::compactionWorkerProvider);
        this.memBlockPool = new NumberMemBlockPool(memPoolSize, getPageSize(), this::createMemSegment);
        this.currentBlock = this.memBlockPool.aquire();
    }

    public void put(JsonNumber jsonNumber, long value) {

        boolean ok = currentBlock.put(memBlockPool.getLongBuffer(), jsonNumber, value);
        if (!ok) {
            compactionManager.submit(0, currentBlock);
            currentBlock = memBlockPool.aquire();
            currentBlock.put(memBlockPool.getLongBuffer(), jsonNumber, value);
        }
    }

    @Override
    public CompactionManager getCompactionManager() {
        return compactionManager;
    }

    @Override
    public Scheduler getCompactionScheduler() {
        return Schedulers.parallel();
    }

    @Override
    public String getName() {
        return this.name;
    }

    private NumberCompactionWorker compactionWorkerProvider(int level) {
        switch (level) {
            case 0:
                return new Level0NumberCompactionWorker(level, this);
            default:
                return new BTreeNumberCompactionWorker(level, this);
        }
    }

    @Override
    public <T extends AbstractBTree> T createBTree(Class<?> type, int totalItemCount) {
        if (type == byte.class) {
            return (T) new ByteBtree(totalItemCount, getPageSize(), this::createMemSegment);
        } else if (type == short.class) {
            return (T) new ShortBtree(totalItemCount, getPageSize(), this::createMemSegment);
        } else if (type == int.class) {
            return (T) new IntBtree(totalItemCount, getPageSize(), this::createMemSegment);
        } else if (type == long.class) {
            return (T) new LongBtree(totalItemCount, getPageSize(), this::createMemSegment);
        } else if (type == double.class) {
            return (T) new DoubleBtree(totalItemCount, getPageSize(), this::createMemSegment);
        } else {
            return null;
        }
    }

    private MemorySegment createMemSegment(MemoryLayout layout) {
        return MemorySegment.allocateNative(layout);
    }

    private MemorySegment createMemSegment(int size) {
        return MemorySegment.allocateNative(size);
    }

    private int getPageSize() {
        return 4096;
    }

    public static void main(String[] args) throws InterruptedException {
        NumberColumnIndex idx = new NumberColumnIndex("test", 5 * 1024 * 1024);
        long l = 0;
        MutableJsonNumber json = new MutableJsonNumber();
        while (l < 10_000_000) {
            json.set(l, 0);
            idx.put(json, l);
            l++;
        }
        Thread.sleep(10000000L);
    }
}
