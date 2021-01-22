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

import com.cloudimpl.db4ji2.core.MergeItem;
import com.cloudimpl.cluster.common.FluxProcessor;
import java.util.stream.IntStream;
import org.agrona.collections.Int2ObjectHashMap;

/** @author nuwansa */
public class StringCompactionManager {
  private final Int2ObjectHashMap<StringCompactionWorker<StringBTree>> map = new Int2ObjectHashMap();
  private final FluxProcessor<MergeItem<? extends StringQueryable>> itemProcessor = new FluxProcessor<>();
  private final StringColumnIndex index;

  public StringCompactionManager(StringColumnIndex index) {
    this.index = index;
  }

  public void init(int levelsCount) {
    register(0, new StringBlockCompactionWorker(index));
    IntStream.range(1, levelsCount)
        .forEach(i -> register(i, new StringBTreeLevelCompactionWorker(i, index)));
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

  public void submit(MergeItem<? extends StringQueryable> item) {
    itemProcessor.add(item);
  }

  private void register(int level, StringCompactionWorker<StringBTree> item) {
    map.put(level, item);
  }
}
