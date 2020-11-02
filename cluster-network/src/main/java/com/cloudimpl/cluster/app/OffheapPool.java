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
package com.cloudimpl.cluster.app;

import io.questdb.std.Unsafe;
import java.util.BitSet;

/**
 *
 * @author nuwansa
 */
public class OffheapPool {

    private final BitSet set;
    private final long startAddr;
    private final int maxBufferSize;

    public OffheapPool(long sizeInBytes, int maxBufferSize) {
        this.startAddr = Unsafe.calloc(sizeInBytes);
        int poolSize = (int) (sizeInBytes / maxBufferSize);
        this.maxBufferSize = maxBufferSize;
        this.set = new BitSet(poolSize);
        this.set.set(0, poolSize);
    }

    public synchronized long getFreeAddr() {
        if(set.isEmpty())
            return -1;
        int val = set.nextSetBit(0);
        long addr = this.startAddr + (val * this.maxBufferSize);
        set.flip(val);
   //     System.out.println("allocate :"+val);
        return addr;
    }

    public synchronized void putAddrBack(long ptr) {
        int i = (int) (ptr - this.startAddr) / this.maxBufferSize;
   //     System.out.println("deallocate :"+i);
        this.set.set(i);
    }

    public long getAddr()
    {
        return this.startAddr;
    }
    
    public static void main(String[] args) {
        OffheapPool pool = new OffheapPool(4L * 1024 * 100000, 4 * 1024);
        BitSet map = new BitSet();
        long start = System.currentTimeMillis();
        long rate = 0;
        while (true) {
            long p = pool.getFreeAddr();
            if (p == -1) {
                while (!map.isEmpty()) {
                    long v = map.nextSetBit(0);
                    map.flip((int)v);
                    pool.putAddrBack(pool.getAddr() +( v * 4096));
                    rate++;
                }
            } else {
                map.set((int)(p  - pool.getAddr())/4096);
                rate++;
            }
            
            long end = System.currentTimeMillis();
            if(end - start >= 1000)
            {
                System.out.println("rate: "+rate);
                rate = 0;
                start = System.currentTimeMillis();
            }
        }
    }
}
