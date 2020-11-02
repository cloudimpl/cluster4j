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

import com.cloudimpl.cluster.network.reactive.NonBlockingHashMapLong;
import io.questdb.std.Os;
import java.util.Random;

/**
 *
 * @author nuwansa
 */
public class MapTest {
    public static void main(String[] args) {
        Os.init();
        Object[] a = new Object[1000000];
        for(int i = 0 ; i < a.length ; i++)
        {
            a[i] = new MapTest();
        }
        
        long[] keys = new long[1000000];
        NonBlockingHashMapLong map = new NonBlockingHashMapLong(keys.length);
        Random r = new Random(System.currentTimeMillis());
        int k = 0;
        while(true)
        {
            keys[k] = r.nextLong();
            map.put(keys[k], a[k]);
            k++;
            if(k >= 100000)
            {
                while(k > 0)
                {
                    map.remove(keys[k]);
                    k--;
                }
            }
        }
    }
}
