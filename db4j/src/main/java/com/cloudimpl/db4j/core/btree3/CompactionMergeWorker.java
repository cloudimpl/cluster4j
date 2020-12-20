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

import com.cloudimpl.cluster.common.FluxProcessor;
import java.io.File;
import reactor.core.scheduler.Schedulers;

/** @author nuwansa */
public class CompactionMergeWorker extends CompactionWorker {

  public CompactionMergeWorker(String idxFolder, int level, int pageSize) {
    super(idxFolder, level, pageSize);
  }

  private BTree merge(BTree left, BTree right) {
    String path = idxFolder + "/" + level + "_" + System.nanoTime() + ".idx";
    System.out.println("compaction:" + path + " started");
    BTree btree = BTree.create(path, left.getSize() + right.getSize(), this.pageSize);
    SortedIterator<Entry> ite = new SortedIterator<>(left.all(), right.all());
    ite.forEachRemaining(e -> btree.put(e.getKey(), e.getValue()));
    btree.complete();
    System.out.println("compaction:" + path + " done");
    if (level < 3) {
      File f = new File(path);
      f.delete();
    }

    return btree;
  }

  @Override
  public CompactionWorker start(FluxProcessor<WorkItem> processor) {
    processor
        .flux()
        .filter(item -> item.getLevel() == level)
        .buffer(2)
        .publishOn(Schedulers.parallel())
        .map(l -> merge(l.get(0).getTarget(), l.get(1).getTarget()))
        .doOnNext(btree -> processor.add(new WorkItem(level + 1, btree)))
        .subscribe();
    return this;
  }
}
