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

import com.cloudimpl.db4ji2.core.LongMemBlock;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;

/** @author nuwansa */
public class DirectLongMemBlockPool implements LongMemBlockPool{
  private final int poolMemSize;
  private final int pageSize;
  private final ManyToManyConcurrentArrayQueue<LongMemBlock> memPool;
  private final ByteBuffer buffer;
  private final LongBuffer longBuffer;
  public DirectLongMemBlockPool(int poolMemSize, int pageSize) {
    this.poolMemSize = poolMemSize;
    this.pageSize = pageSize;
    memPool = new ManyToManyConcurrentArrayQueue<>(this.poolMemSize / this.pageSize);
    buffer = ByteBuffer.allocateDirect(this.poolMemSize);
    longBuffer = buffer.asLongBuffer();
    allocate();
  }

  private void allocate() {
    int blocks = this.poolMemSize / this.pageSize;
    int i = 0;
    while (i < blocks) {
      memPool.add(new LongMemBlock(buffer, i * pageSize, pageSize));
      i++;
    }
  }

  @Override
  public LongBuffer getLongBuffer() {
    return longBuffer;
  }

  @Override
  public LongMemBlock aquire() {
    return memPool.poll();
  }

  @Override
  public void release(LongMemBlock block) {
    block.updateSize(0);
    memPool.add(block);
  }
}
