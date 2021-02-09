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
package com.cloudimpl.db4ji2.idx.lng.old;

import com.cloudimpl.db4ji2.core.old.LongEntry;
import com.google.common.collect.Iterators;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** @author nuwansa */
public class LongQueryBlockAggregator {
  private final Supplier<Stream<LongQueryable>> supplier;

  public LongQueryBlockAggregator(Supplier<Stream<LongQueryable>> supplier) {
    this.supplier = supplier;
  }

  public LongQueryBlockAggregator() {
    this.supplier = () -> this.getBlockStream();
  }

  protected Stream<LongQueryable> getBlockStream() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  public java.util.Iterator<LongEntry> all(boolean asc) {
    Iterable<Iterator<LongEntry>> itarable = () -> supplier.get().map(m -> m.<LongEntry>all(asc)).iterator();
    Comparator<LongEntry> comp = (LongEntry o1, LongEntry o2) -> Long.compare(o1.getKey(), o2.getKey());
    if (!asc) {
      comp = comp.reversed();
    }
    return Iterators.mergeSorted(itarable, comp);
  }

  public java.util.Iterator<LongEntry> findEQ(long key) {
    Iterable<Iterator<LongEntry>> itarable = () -> supplier.get().map(m -> m.<LongEntry>findEQ(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (LongEntry o1, LongEntry o2) -> Long.compare(o1.getKey(), o2.getKey()));
  }

  public java.util.Iterator<LongEntry> findGE(long key) {
    Iterable<Iterator<LongEntry>> itarable = () -> supplier.get().map(m -> m.<LongEntry>findGE(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (LongEntry o1, LongEntry o2) -> Long.compare(o1.getKey(), o2.getKey()));
  }

  public java.util.Iterator<LongEntry> findGT(long key) {
    Iterable<Iterator<LongEntry>> itarable = () -> supplier.get().map(m -> m.<LongEntry>findGT(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (LongEntry o1, LongEntry o2) -> Long.compare(o1.getKey(), o2.getKey()));
  }

  public java.util.Iterator<LongEntry> findLE(long key) {
    Iterable<Iterator<LongEntry>> itarable = () -> supplier.get().map(m -> m.<LongEntry>findLE(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (LongEntry o1, LongEntry o2) -> Long.compare(o2.getKey(), o1.getKey()));
  }

  public java.util.Iterator<LongEntry> findLT(long key) {
    Iterable<Iterator<LongEntry>> itarable = () -> supplier.get().map(m -> m.<LongEntry>findLT(key)).iterator();
    return Iterators.mergeSorted(
        itarable, (LongEntry o1, LongEntry o2) -> Long.compare(o2.getKey(), o1.getKey()));
  }
}
