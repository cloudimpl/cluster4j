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

import java.util.Arrays;
import java.util.Random;
import java.util.stream.LongStream;

/**
 *
 * @author nuwansa
 */
public class BSearchTest {

    public static void main(String[] args) {
        Random r = new Random(System.currentTimeMillis());
        long[] array = LongStream.range(0, 50000).map(i -> r.nextLong() % 50000).toArray();
        int hits = 0;
        int rate = 0;
        long start = System.currentTimeMillis();
        while (true) {
            Arrays.sort(array);
            long key = r.nextLong() % 50000;
            int k = Arrays.binarySearch(array, key);
            if (k >= 0 && array[k] == key) {
                hits++;
            }
            rate++;
            long end = System.currentTimeMillis();
            if (end - start >= 1000) {
                System.out.println("hits: " + hits + " rate: "+rate);
                start = System.currentTimeMillis();
                rate = 0;
                hits = 0;
            }
        }
    }
}
