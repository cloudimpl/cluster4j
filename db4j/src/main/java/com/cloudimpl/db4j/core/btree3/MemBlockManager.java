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

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;

/** @author nuwansa */
public class MemBlockManager {
  public ManyToManyConcurrentArrayQueue<MemBlock> queue;

  public MemBlockManager(int blockCount) {
    init(blockCount);
  }

  private void init(int blockCount) {
    queue = new ManyToManyConcurrentArrayQueue<>(blockCount);
    int i = 0;
    while (i < blockCount) {
      queue.add(new MemBlock(this, 255, 8192, k -> null));
      i++;
    }
  }

  public MemBlock get() {
    return queue.poll();
  }

  public void release(MemBlock block) {
    queue.add(block);
  }
}