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
package test;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;

/** @author nuwansa */
public class NumberMemBlockPool{
  private final int poolMemSize;
  private final int pageSize;
  private final ManyToManyConcurrentArrayQueue<NumberMemBlock> memPool;
  private final ByteBuffer buffer;
  private final LongBuffer longBuffer;
  private final MemorySegmentProvider provider;
  public NumberMemBlockPool(int poolMemSize, int pageSize,MemorySegmentProvider provider) {
    this.poolMemSize = poolMemSize;
    this.pageSize = pageSize;
    this.provider = provider;
    memPool = new ManyToManyConcurrentArrayQueue<>(this.poolMemSize / this.pageSize);
    buffer = provider.apply(this.poolMemSize).asByteBuffer();
    longBuffer = buffer.asLongBuffer();
    allocate();
  }

  private void allocate() {
    int blocks = this.poolMemSize / this.pageSize;
    int i = 0;
    while (i < blocks) {
      memPool.add(new NumberMemBlock(buffer, i * pageSize, pageSize));
      i++;
    }
  }

  public LongBuffer getLongBuffer() {
    return longBuffer;
  }

  public LongMemBlock aquire() {
    return memPool.poll();
  }

  public void release(NumberMemBlock block) {
    block.updateSize(0);
    memPool.add(block);
  }
}
