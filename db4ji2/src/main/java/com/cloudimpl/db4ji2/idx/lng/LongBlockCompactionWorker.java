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
package com.cloudimpl.db4ji2.idx.lng;

import com.cloudimpl.db4ji2.idx.lng.LongColumnIndex;
import com.cloudimpl.db4ji2.core.MergeItem;
import com.cloudimpl.db4ji2.core.LongBTree;
import java.util.List;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** @author nuwansa */
public class LongBlockCompactionWorker extends LongCompactionWorker<LongBTree> {

  public LongBlockCompactionWorker(LongColumnIndex idx) {
    super(0, idx);
  }

  @Override
  public Mono<MergeItem<LongBTree>> compact(List<MergeItem<? extends LongQueryable>> items) {
    return Mono.just(items).publishOn(Schedulers.parallel()).map(this::merge);
  }

  private MergeItem<LongBTree> merge(List<MergeItem<? extends LongQueryable>> items) {
    //   System.out.println("level:" + getLevel() + ":started");
    int totalItems = items.stream().map(m -> m.getItem()).mapToInt(LongQueryable::getSize).sum();
    LongBTree btree = getIdx().createBTree(totalItems);//LongBTree.create(totalItems, 4096,getIdx().getComparator(),getIdx().getEntrySupplier());
    LongQueryBlockAggregator blockMan =
        new LongQueryBlockAggregator(() -> items.stream().map(m -> m.getItem()));
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
    return 255;
  }
}
