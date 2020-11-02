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
package com.cloudimpl.cluster.network.reactive.test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;

/**
 *
 * @author nuwansa
 */
public class FluxTest {

    public static void main(String[] args) throws InterruptedException {
//        Flux.interval(Duration.ofMillis(10)).groupBy(i->i).flatMap(f->f)
//              //  .doOnNext(i->System.out.println("i:"+i))
//                .subscribe();
//        Thread.sleep(100000);
        Map<Long, Object> map = new ConcurrentHashMap<>();
        Object[] values = new Object[1000000];
        for (int i = 0; i < 1000000; i++) {
            values[i] = "nuwan" + i;
        }

        ChronicleMap<LongValue, Object> orders = ChronicleMap
                .of(LongValue.class, Object.class)
                .name("orders-map")
                .averageValue(1L)
                .entries(1_000_000)
                .create();

        Random r = new Random(System.currentTimeMillis());
        int i = 0;
        int rate = 0;
        long start = System.currentTimeMillis();
        LongValue key = Values.newHeapInstance(LongValue.class);
        while (true) {
            key.setValue(r.nextLong() % 1000000);
            //map.put(r.nextLong() % 1000000, values[i]);
            orders.put(key, values[i]);
            i++;
            if (i >= 1000000) {
                i = 0;
            }
           // map.remove(r.nextLong() % 1000000);
           key.setValue(r.nextLong() % 1000000);
           orders.remove(key);
            rate++;
            long end = System.currentTimeMillis();
            if (end - start >= 1000) {
                start = System.currentTimeMillis();
                System.out.println("rate: " + rate);
                rate = 0;
            }
        }
    }
}
