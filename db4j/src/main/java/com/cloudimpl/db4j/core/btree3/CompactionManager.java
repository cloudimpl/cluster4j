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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/** @author nuwansa */
public class CompactionManager {
  private final FluxProcessor<WorkItem> fluxPrcessor;
  private final Map<Integer, CompactionWorker> workers;
  private final String idxFolder;
  private final int pageSize;

  public CompactionManager(String idxFolder, int pageSize) {
    this.idxFolder = idxFolder;
    this.pageSize = pageSize;
    fluxPrcessor = new FluxProcessor<>();
    workers = new HashMap<>();
  }

  public void add(BTree tree) {
    fluxPrcessor.add(new WorkItem(0, tree));
  }

  public void start() {
    workers.put(0, new Level0CompactionWorker(idxFolder, 0, pageSize).start(fluxPrcessor));
    IntStream.range(1, 10)
        .forEach(
            i ->
                workers.put(
                    i, new CompactionMergeWorker(idxFolder, i, pageSize).start(fluxPrcessor)));
  }
}
