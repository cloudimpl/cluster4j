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
package com.cloudimpl.db4j.core.btree_1;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/** @author nuwansa */
public class BTreeIndex {

  private BPlusTree[] pages;
  private int itemCount;
  private int itemPerTree;
  private int index;
  private BPlusTree currentTree;

  public BTreeIndex(int itemCount, int itemPerTree, int itemPerNode) {
    this.itemCount = itemCount;
    this.itemPerTree = itemPerTree;
    int treeCount = (int) Math.ceil((double) itemCount / itemPerTree);
    pages = new BPlusTree[treeCount];
    init(itemPerNode);
    this.index = 0;
    this.currentTree = pages[index];
  }

  public BPlusTree get(int i) {
    return pages[i];
  }

  private void init(int itemPerNode) {
    int i = 0;
    while (i < pages.length) {
      pages[i] = new BPlusTree(itemPerTree, itemPerNode);
      i++;
    }
  }

  public boolean put(long key, long value) {
    boolean ok = this.currentTree.put(key, value);
    if (!ok) {
      this.currentTree.complete();
      //      System.out.println("going to next map:" + key);
      this.currentTree = pages[++index];
      ok = this.currentTree.put(key, value);
    }
    return ok;
  }

  public void complete() {
    this.currentTree.complete();
  }

  public Flux<Long> findGt(long key, int topN) {
    System.out.println("searching:" + this.pages.length + " indexes");
    return Flux.fromArray(this.pages)
        //    .take(this.pageIndex)
        //     .filter(p -> key < p.max)
        .parallel()
        .runOn(Schedulers.parallel())
        .flatMap(
            p -> p.findGt(key, topN)
            //  .doOnSubscribe(s -> System.out.println("start:" + p.items.arrayOffset()))
            //   .doOnTerminate(() -> System.out.println("end:" + p.items.arrayOffset()))
            )
        .sequential()
        .sort()
        // .sequential()
        //  .collectSortedList((l1, l2) -> Integer.compare(l1.hashCode(), l2.hashCode()))
        //   .doOnNext(l -> System.out.println("size: " + l.size()))
        //    .flatMapMany(list -> Flux.<Item>mergeOrdered(list.toArray(Flux[]::new)))
        .take(topN == -1 ? Long.MAX_VALUE : topN);
  }

  public static void main(String[] args) throws InterruptedException {
    int vol = 10_000_000;
    int itemPerTree = 253 * 256;
    List<Long> list = IntStream.range(0, vol).mapToObj(i -> (long) i).collect(Collectors.toList());
    Collections.shuffle(list);

    // list = Arrays.asList(new Long[] {3L, 4L, 5L, 8L, 9L, 0L, 1L, 2L, 6L, 7L});
    BTreeIndex idx = new BTreeIndex(vol, itemPerTree, 253);
    Flux.fromIterable(list)
        .buffer(itemPerTree)
        .doOnNext(l -> l.sort(Comparator.naturalOrder()))
        //    .doOnNext(System.out::println)
        .flatMapIterable(l -> l)
        .doOnNext(i -> idx.put(i, i * 10))
        .doOnError(err -> err.printStackTrace())
        //      .doOnNext(System.out::println)
        .doOnTerminate(() -> idx.complete())
        .subscribe();
    // Thread.sleep(10000);
    // idx.findGt(2, 10).doOnNext(r -> System.out.println("xxx:" + r)).subscribe();

    Random r = new Random(System.currentTimeMillis());
    AtomicLong s = new AtomicLong();
    Flux.interval(Duration.ofSeconds(1))
        .doOnSubscribe(k -> s.set(System.currentTimeMillis()))
        .take(100)
        .map(k -> list.get(r.nextInt(vol)))
        .doOnNext(k -> System.out.println("start searching : " + k))
        .doOnNext(k -> s.set(System.currentTimeMillis()))
        .flatMap(
            k ->
                idx.findGt(k, 1)
                    //     .doOnNext(System.out::println)
                    .doOnTerminate(
                        () ->
                            System.out.println(
                                "terminated:" + (System.currentTimeMillis() - s.get()))))
        //  .sort((Item left, Item right) -> Long.compare(left.getKey(), right.getKey()))
        //       .doOnNext(k -> System.out.println("kgt:" + k))
        .doOnError(thr -> thr.printStackTrace())
        .doOnTerminate(
            () -> System.out.println("terminated:" + (System.currentTimeMillis() - s.get())))
        .subscribe();
    //    BPlusTree btree = new BPlusTree(5, 2);
    //    btree.put(2, 20);
    //    btree.put(3, 30);
    //    btree.put(4, 40);
    //    btree.put(6, 60);
    //    btree.put(9, 90);
    //    btree.complete();
    //
    //    btree.findGt(2, 10).doOnNext(r -> System.out.println("xxx:" + r)).subscribe();
    Thread.sleep(100000);
  }
}
