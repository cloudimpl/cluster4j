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

import com.cloudimpl.cluster.common.FluxProcessor;
import java.util.stream.IntStream;
import org.agrona.collections.Int2ObjectHashMap;

/** @author nuwansa */
public class CompactionManager {
  private final Int2ObjectHashMap<CompactionWorker<BTree>> map = new Int2ObjectHashMap();
  private final FluxProcessor<MergeItem<? extends Queryable>> itemProcessor = new FluxProcessor<>();
  private final ColumnIndex index;

  public CompactionManager(ColumnIndex index) {
    this.index = index;
  }

  public void init(int levelsCount) {
    register(0, new BlockCompactionWorker(index));
    IntStream.range(1, levelsCount)
        .forEach(i -> register(i, new BTreeLevelCompactionWorker(i, index)));
    map.values()
        .forEach(
            worker ->
                itemProcessor
                    .flux()
                    .doOnNext(
                        item -> {
                          if (!map.containsKey(item.getLevel()))
                            throw new RuntimeException(
                                "level not found to compaction : " + item.getLevel());
                        })
                    .filter(item -> item.getLevel() == worker.getLevel())
                    .buffer(worker.getBatchCount())
                    .flatMap(items -> worker.compact(items))
                    .doOnNext(item -> submit(item))
                    .doOnError(thr -> thr.printStackTrace())
                    .subscribe());
  }

  public void submit(MergeItem<? extends Queryable> item) {
    itemProcessor.add(item);
  }

  private void register(int level, CompactionWorker<BTree> item) {
    map.put(level, item);
  }
}
