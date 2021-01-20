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
package com.cloudimpl.db4ji2.idx.str;

import com.cloudimpl.db4ji2.idx.str.StringQueryable;
import com.google.common.collect.Iterators;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** @author nuwansa */
public class StringQueryBlockAggregator {
  private final Supplier<Stream<StringQueryable>> supplier;

  public StringQueryBlockAggregator(Supplier<Stream<StringQueryable>> supplier) {
    this.supplier = supplier;
  }

  public StringQueryBlockAggregator() {
    this.supplier = () -> this.getBlockStream();
  }

  protected Stream<StringQueryable> getBlockStream() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  public java.util.Iterator<StringEntry> all(boolean asc) {
    Iterable<Iterator<StringEntry>> itarable = () -> supplier.get().map(m -> m.<StringEntry>all(asc)).iterator();
    Comparator<StringEntry> comp = StringEntry::compare;
    if (!asc) {
      comp = comp.reversed();
    }
    return Iterators.mergeSorted(itarable, comp);
  }

  public java.util.Iterator<StringEntry> findEQ(CharSequence key) {
    Iterable<Iterator<StringEntry>> itarable = () -> supplier.get().map(m -> m.<StringEntry>findEQ(key)).iterator();
    return Iterators.mergeSorted(
        itarable,StringEntry::compare);
  }

  public java.util.Iterator<StringEntry> findGE(CharSequence key) {
    Iterable<Iterator<StringEntry>> itarable = () -> supplier.get().map(m -> m.<StringEntry>findGE(key)).iterator();
    return Iterators.mergeSorted(
        itarable, StringEntry::compare);
  }

  public java.util.Iterator<StringEntry> findGT(CharSequence key) {
    Iterable<Iterator<StringEntry>> itarable = () -> supplier.get().map(m -> m.<StringEntry>findGT(key)).iterator();
    return Iterators.mergeSorted(
        itarable,StringEntry::compare);
  }

  public java.util.Iterator<StringEntry> findLE(CharSequence key) {
    Iterable<Iterator<StringEntry>> itarable = () -> supplier.get().map(m -> m.<StringEntry>findLE(key)).iterator();
    return Iterators.mergeSorted(
        itarable, StringEntry::compare);
  }

  public java.util.Iterator<StringEntry> findLT(CharSequence key) {
    Iterable<Iterator<StringEntry>> itarable = () -> supplier.get().map(m -> m.<StringEntry>findLT(key)).iterator();
    return Iterators.mergeSorted(
        itarable, StringEntry::compare);
  }
}
