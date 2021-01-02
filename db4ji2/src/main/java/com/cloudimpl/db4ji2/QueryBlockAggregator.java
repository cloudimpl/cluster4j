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
package com.cloudimpl.db4ji2;

import com.google.common.collect.Iterators;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** @author nuwansa */
public class QueryBlockAggregator {
  private final Supplier<Stream<Queryable>> supplier;

  public QueryBlockAggregator(Supplier<Stream<Queryable>> supplier) {
    this.supplier = supplier;
  }

  public QueryBlockAggregator() {
    this.supplier = () -> this.getBlockStream();
  }

  protected Stream<Queryable> getBlockStream() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  public java.util.Iterator<Entry> all(boolean asc) {
    Iterable<Iterator<Entry>> itarable = () -> supplier.get().map(m -> m.all(asc)).iterator();
    Comparator<Entry> comp = (Entry o1, Entry o2) -> Long.compare(o1.getKey(), o2.getKey());
    if (!asc) {
      comp = comp.reversed();
    }
    return Iterators.mergeSorted(itarable, comp);
  }

  public java.util.Iterator<Entry> findEQ(long key) {
    Iterable<Iterator<Entry>> itarable = () -> supplier.get().map(m -> m.findEQ(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (Entry o1, Entry o2) -> Long.compare(o1.getKey(), o2.getKey()));
  }

  public java.util.Iterator<Entry> findGE(long key) {
    Iterable<Iterator<Entry>> itarable = () -> supplier.get().map(m -> m.findGE(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (Entry o1, Entry o2) -> Long.compare(o1.getKey(), o2.getKey()));
  }

  public java.util.Iterator<Entry> findGT(long key) {
    Iterable<Iterator<Entry>> itarable = () -> supplier.get().map(m -> m.findGT(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (Entry o1, Entry o2) -> Long.compare(o1.getKey(), o2.getKey()));
  }

  public java.util.Iterator<Entry> findLE(long key) {
    Iterable<Iterator<Entry>> itarable = () -> supplier.get().map(m -> m.findLE(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (Entry o1, Entry o2) -> Long.compare(o2.getKey(), o1.getKey()));
  }

  public java.util.Iterator<Entry> findLT(long key) {
    Iterable<Iterator<Entry>> itarable = () -> supplier.get().map(m -> m.findLT(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (Entry o1, Entry o2) -> Long.compare(o2.getKey(), o1.getKey()));
  }
}
