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

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import jdk.incubator.foreign.MemoryLayout;
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
    MemoryManager man = new UnsafeMemoryManager();

    public AtomicLong size = new AtomicLong(0);
    public NumberColumnIndex(String name, int memPoolSize) {
        this.name = name;
        compactionManager = new CompactionManager(10, this, this::compactionWorkerProvider);
        this.memBlockPool = new NumberMemBlockPool(memPoolSize, getPageSize(), this::createMemSegment);
        this.currentBlock = this.memBlockPool.aquire();
        size.addAndGet(this.currentBlock.memSize());
    }

    public void put(JsonNumber jsonNumber, long value) {

        boolean ok = currentBlock.put(memBlockPool.getLongBuffer(), jsonNumber, value);
        if (!ok) {
            compactionManager.submit(0, currentBlock);
            currentBlock = memBlockPool.aquire();
            while (currentBlock == null) {
                currentBlock = memBlockPool.aquire();
            }
            size.addAndGet(this.currentBlock.memSize());
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
                return new Level0NumberCompactionWorker(level, this, this::recycle);
            default:
                return new BTreeNumberCompactionWorker(level, this, this::recycle);
        }
    }

    private void recycle(QueryBlock queryBlock) {
        if (queryBlock instanceof Number2MemBlock) {
            memBlockPool.release((Number2MemBlock) queryBlock);
        }else
        {
        //    queryBlock.close();
        }
        size.addAndGet(-queryBlock.memSize());
    }

    @Override
    public <T extends AbstractBTree> T createBTree(Class<?> type, int totalItemCount) {
        T t =  _createBTree(type, totalItemCount);
        size.addAndGet(t.memSize());
        return t;
    }
    
    
    public <T extends AbstractBTree> T _createBTree(Class<?> type, int totalItemCount) {
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

    private OffHeapMemory createMemSegment(MemoryLayout layout) {
        return createMemSegment(layout.byteSize());
    }
    AtomicInteger i = new AtomicInteger(0);
    private OffHeapMemory createMemSegment(long size) {
        return man.mapFromPath(Path.of("/Users/nuwan/data/"+i.incrementAndGet()+".data"), 0, size, FileChannel.MapMode.READ_WRITE);
    }

    private int getPageSize() {
        return 4096;
    }

    public static void main(String[] args) throws InterruptedException {
        NumberColumnIndex idx = new NumberColumnIndex("test", 100 * 1024 * 1024);
        long l = 0;
        MutableJsonNumber json = new MutableJsonNumber();

        long start = System.currentTimeMillis();
        int rate = 0;
        try {
            while (l < Long.MAX_VALUE) {
                json.set(l, 0);
                idx.put(json, l);
                l++;
                rate++;
                long end = System.currentTimeMillis();
                if(end - start >= 1000)
                {
                    System.out.println("rate: "+rate + " L "+l +"  "+  idx.size.get());
                    rate = 0;
                    start = System.currentTimeMillis();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("***************************************************************l:" + l + " time : "+(end - start));
        while(true)
        {
            System.out.println("memsize: "+idx.size.get() + " l : "+l);
            Thread.sleep(1000L);
        }
        
    }
}
