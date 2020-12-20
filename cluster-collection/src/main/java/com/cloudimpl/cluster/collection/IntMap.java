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
package com.cloudimpl.cluster.collection;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author nuwansa
 */
public interface IntMap {
    int size();

    boolean isEmpty();

    boolean containsKey(String key);

    boolean containsValue(int value);

    int get(String key);

    int put(String key, int value);

    int remove(String key);

    void putAll(IntMap m);

    void clear();

    Set<String> keySet();

    Collection<Integer> values();

    Set<Map.Entry<String, Integer>> entrySet();
    
}
