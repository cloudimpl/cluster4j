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
package com.cloudimpl.db4j.core_old;

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import reactor.core.publisher.Flux;

/** @author nuwansa */
public class MemNumberColumnStore extends NumberColumnStore {

  private final String tablePath;
  private final DB db;
  private final NavigableSet<Object[]> map;

  public MemNumberColumnStore(String name, String tablePath) {
    this.tablePath = tablePath;
    this.db =
        DBMaker.fileDB(tablePath + "/" + name + ".cidx")
            .fileMmapEnable()
            .transactionEnable()
            .make();
    this.map =
        this.db
            .treeSet("map")
            .serializer(new SerializerArrayTuple(Serializer.LONG, Serializer.LONG))
            .counterEnable()
            .createOrOpen();
  }

  public void add(long key, long value) {
    this.map.add(new Object[] {key, value});
  }

  public void commit() {
    this.db.commit();
  }

  @Override
  public Flux<Long> find(String value) {

    return Flux.fromIterable(
            () ->
                this.map
                    .subSet(
                        new Object[] {Long.valueOf(value)},
                        new Object[] {Long.valueOf(value), null})
                    .iterator())
        .map(arr -> arr[0])
        .cast(Long.class);
  }

  @Override
  public Flux<Long> findGT(String value) {
    return Flux.fromIterable(
            () -> this.map.tailSet(new Object[] {Long.valueOf(value)}, false).iterator())
        .map(arr -> arr[0])
        .cast(Long.class);
  }

  @Override
  public Flux<Long> findGTE(String value) {
    return Flux.fromIterable(() -> this.map.tailSet(new Object[] {Long.valueOf(value)}).iterator())
        .map(arr -> arr[0])
        .cast(Long.class);
  }

  @Override
  public Flux<Long> findLT(String value) {
    return Flux.fromIterable(() -> this.map.headSet(new Object[] {Long.valueOf(value)}).iterator())
        .map(arr -> arr[0])
        .cast(Long.class);
  }

  @Override
  public Flux<Long> findLTE(String value) {
    return Flux.fromIterable(
            () -> this.map.headSet(new Object[] {Long.valueOf(value)}, true).iterator())
        .map(arr -> arr[0])
        .cast(Long.class);
  }

  @Override
  public long getSizeInBytes() {
    return this.map.size() * 20;
  }

  public static void main(String[] args) {
    MemNumberColumnStore store = new MemNumberColumnStore("Test", "/Users/nuwansa/");
    int vol = 20_000_00;
    List<Long> list = IntStream.range(0, vol).mapToObj(i -> (long) i).collect(Collectors.toList());
    Collections.shuffle(list);

    int rate = 0;
    long start = System.currentTimeMillis();
    int i = 0;
    while (i < vol) {
      store.add(list.get(i), i);
      i++;
      rate++;
      long end = System.currentTimeMillis();
      if (end - start >= 1000) {
        System.out.println("rate: " + rate);
        rate = 0;
        start = System.currentTimeMillis();
      }
    }
    long s = System.currentTimeMillis();
    store
        .findGT("7979")
        .take(10)
        .doOnTerminate(() -> System.out.println(System.currentTimeMillis() - s))
        .subscribe();
    //    store.add(new BigDecimal("10"), 100);
    //    store.add(new BigDecimal("7"), 200);
    //    store.add(new BigDecimal("2"), 500);
    //    store.add(new BigDecimal("8"), 700);
    //    store.add(new BigDecimal("9"), 800);
    //    store.add(new BigDecimal("2"), 1000);
    //    store.commit();
    //    store
    //        .find("10")
    //        .doOnNext(System.out::print)
    //        .doOnNext(l -> System.out.print(","))
    //        .doOnTerminate(() -> System.out.println())
    //        .subscribe();
    //    store
    //        .find("2")
    //        .doOnNext(System.out::print)
    //        .doOnNext(l -> System.out.print(","))
    //        .doOnTerminate(() -> System.out.println())
    //        .subscribe();
    //
    //    store
    //        .findGTE("2")
    //        .doOnNext(System.out::print)
    //        .doOnNext(l -> System.out.print(","))
    //        .doOnTerminate(() -> System.out.println())
    //        .subscribe();
    //
    //    store
    //        .findGT("2")
    //        .doOnNext(System.out::print)
    //        .doOnNext(l -> System.out.print(","))
    //        .doOnTerminate(() -> System.out.println())
    //        .subscribe();
    //
    //    store
    //        .findLT("7")
    //        .doOnNext(System.out::print)
    //        .doOnNext(l -> System.out.print(","))
    //        .doOnTerminate(() -> System.out.println())
    //        .subscribe();
    //
    //    store
    //        .findLTE("2")
    //        .doOnNext(System.out::print)
    //        .doOnNext(l -> System.out.print(","))
    //        .doOnTerminate(() -> System.out.println())
    //        .subscribe();
  }
}
