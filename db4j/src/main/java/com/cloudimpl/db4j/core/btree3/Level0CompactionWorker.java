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
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

/** @author nuwansa */
public class Level0CompactionWorker extends CompactionWorker {

  public Level0CompactionWorker(String idxFolder, int level, int pageSize) {
    super(idxFolder, level, pageSize);
  }

  @Override
  public CompactionWorker start(FluxProcessor<WorkItem> processor) {
    processor
        .flux()
        .filter(item -> item.getLevel() == 0)
        .map(item -> item.<MemBlock>getTarget())
        .buffer(255)
        .map(this::merge)
        .doOnNext(btree -> processor.add(new WorkItem(level + 1, btree)))
        .subscribe();
    return this;
  }

  private BTree merge(List<MemBlock> list) {
    Iterable<Iterator<Entry>> itarable =
        () -> StreamSupport.stream(list.spliterator(), false).map(m -> m.iterator()).iterator();
    Iterator<Entry> ite =
        Iterators.mergeSorted(
            itarable, (Entry o1, Entry o2) -> Long.compare(o1.getKey(), o2.getKey()));
    BTree tree = BTree.create(65025, 4096);
    ite.forEachRemaining(e -> tree.put(e.getKey(), e.getKey()));
    return tree;
  }
}
