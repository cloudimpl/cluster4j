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
package com.cloudimpl.msg.lib;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author nuwan
 */
public class XLongIndex {
    public static void main(String[] args) {
        System.out.println(BigDecimal.valueOf(10, 2).toString());
        Map<String,Object> map = new ConcurrentHashMap<>();
        String[] keys = new String[100000];
        int i = 0;
        while(i < keys.length)
        {
            keys[i] = "key"+i;
            map.put(keys[i], "value"+i);
            i++;
        }
        
        long start = System.currentTimeMillis();
        i = 0;
        long j = 0;
        while(j < 10000000000L)
        {
            map.get(keys[i]);
            i++;
            if(i == keys.length)
                i = 0;
            
            j++;
        }
        long end = System.currentTimeMillis();
        System.out.println((((double)(end - start))/j) * 1000000);
    }
}
