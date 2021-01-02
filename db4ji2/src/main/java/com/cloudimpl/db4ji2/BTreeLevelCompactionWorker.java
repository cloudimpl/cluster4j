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

import java.util.List;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** @author nuwansa */
public class BTreeLevelCompactionWorker extends CompactionWorker<BTree> {

  public static final int[] arr = {10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};

  private boolean activate = false;

  private long totalItemProcessed = 0;

  public BTreeLevelCompactionWorker(int level, ColumnIndex idx) {
    super(level, idx);
  }

  @Override
  public Mono<MergeItem<BTree>> compact(List<MergeItem<? extends Queryable>> items) {
    //    if (items.size() > getBatchCount()) {
    //      return Mono.error(new CompactionException("compaction item count exceed: " +
    // items.size()));
    //    }
    return Mono.just(items).publishOn(Schedulers.parallel()).map(i -> this.merge(i));
  }

  //  private MergeItem<BTree> merge(Queryable tree1, Queryable tree2) {
  //    System.out.println("level:" + getLevel() + ":started");
  //    BTree btree = BTree.create(tree1.getSize() + tree2.getSize(), 4096);
  //    SortedIterator<Entry> ite = new SortedIterator<>(tree1.all(true), tree2.all(true));
  //    ite.forEachRemaining(e -> btree.put(e.getKey(), e.getValue()));
  //    btree.complete();
  //    //    System.out.println(
  //    //        "level:"
  //    //            + getLevel()
  //    //            + " : compaction done : "
  //    //            + (tree1.getSize() + tree2.getSize())
  //    //            + "TID: "
  //    //            + Thread.currentThread().getId());
  //    getIdx().update(Arrays.asList(tree1, tree2), btree);
  //    getIdx().release(tree1);
  //    getIdx().release(tree2);
  //    System.out.println("level:" + getLevel() + ":end - > size:" + btree.getSize());
  //    return new MergeItem<>(getLevel() + 1, btree);
  //  }

  private MergeItem<BTree> merge(List<MergeItem<? extends Queryable>> items) {
    if (!activate) {
      System.out.println("level :" + getLevel() + " activated");
      activate = true;
    }
    //   System.out.println("level:" + getLevel() + ":started");
    //  items.forEach(i -> System.out.println("Level:" + getLevel() + " : Itemlevel: " +
    // i.getLevel()));
    int totalItems = items.stream().map(m -> m.getItem()).mapToInt(Queryable::getSize).sum();
    totalItemProcessed += totalItems;
    //  if (getLevel() == 2)
    //    {
    //      System.out.println(
    //          "level:"
    //              + getLevel()
    //              + "total Items : "
    //              + totalItems
    //              + " blocks : "
    //              + items.size()
    //              + " total processed :"
    //              + totalItemProcessed);
    //    }
    BTree btree = BTree.create(totalItems, 4096);
    QueryBlockAggregator blockMan =
        new QueryBlockAggregator(() -> items.stream().map(m -> m.getItem()));
    blockMan.all(true).forEachRemaining(e -> btree.put(e.getKey(), e.getValue()));
    btree.complete();
    //    System.out.println(
    //        "level:"
    //            + getLevel()
    //            + " : compaction done : "
    //            + totalItems
    //            + "THID:"
    //            + Thread.currentThread().getId());
    getIdx().update(items.stream().map(m -> m.getItem()).collect(Collectors.toList()), btree);
    items.stream().map(i -> i.getItem()).forEach(i -> getIdx().release(i));
    //  System.out.println("level:" + getLevel() + ":end");
    return new MergeItem<>(getLevel() + 1, btree);
  }

  @Override
  public int getBatchCount() {
    return arr[getLevel() - 1];
    // return 2;
  }
}
