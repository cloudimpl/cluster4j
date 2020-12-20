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
package com.cloudimpl.db4j.core.btree3;

import java.util.Map;

/** @author nuwansa */
public class Entry implements Map.Entry<Long, Long>, Comparable<Entry> {
  private final long key;
  private final long value;

  public Entry(long key, long value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public Long getKey() {
    return this.key;
  }

  @Override
  public Long getValue() {
    return this.value;
  }

  @Override
  public Long setValue(Long value) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public String toString() {
    return "Entry{" + "key=" + key + ", value=" + value + '}';
  }

  @Override
  public int compareTo(Entry o) {
    return Long.compare(getKey(), ((Entry) o).getKey());
  }
}
