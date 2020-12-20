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

/** @author nuwansa */
public class BTreeIndex {

  private final CompactionManager compactionManager;
  private BTree btree;

  public BTreeIndex(String idxFolder) {
    this.compactionManager = new CompactionManager(idxFolder, 4096);
    this.btree = BTree.create(65025, 4096);
    this.compactionManager.start();
  }

  public void put(long key, long value) {
    if (!this.btree.put(key, value)) {
      this.btree.complete();
      compactionManager.add(btree);
      this.btree = BTree.create(65025, 4096);
      this.btree.put(key, value);
    }
  }

  public void complete() {
    this.btree.complete();
    compactionManager.add(btree);
  }

  public static void main(String[] args) throws InterruptedException {
    BTreeIndex idx = new BTreeIndex("/Users/nuwansa/data");
    int i = 0;
    int vol = 10_0000_0;
    while (i < vol) {
      idx.put(i, i * 10);
      i++;
    }
    idx.complete();
    Thread.sleep(1000000000);
  }
}
