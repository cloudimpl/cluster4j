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
package com.cloudimpl.db4ji2.core;

/**
 * @author nuwansa
 */
public class LongEntry implements Comparable<LongEntry>, Entry {

    private long key;
    private long value;

    public LongEntry(long key, long value) {
        this.key = key;
        this.value = value;
    }

    public LongEntry() {
    }

    // @Override
    public long getKey() {
        return this.key;
    }

    //  @Override
    @Override
    public long getValue() {
        return this.value;
    }

    @Override
    public LongEntry set(long key, long value) {
        this.key = key;
        this.value = value;
        return this;
    }

    @Override
    public String toString() {
        return "Entry{" + "key=" + key + ", value=" + value + '}';
    }

    @Override
    public int compareTo(LongEntry o) {
        return Long.compare(getKey(), ((LongEntry) o).getKey());
    }

    @Override
    public long _getKey() {
        return this.key;
    }
}
