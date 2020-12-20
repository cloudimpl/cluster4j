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
package com.cloudimpl.db4j.core.impl;

import java.nio.LongBuffer;

/** @author nuwansa */
public class LongMemBlock extends ByteBufMemBlock {

  public LongMemBlock(int offset, int pageSize) {
    super(offset / 8, (pageSize / 16) - 1);
  }

  public int allocate(LongBuffer longBuffer, int pageSize) {
    longBuffer.position(offset).put(offset);
  }

  @Override
  protected int getKeySize() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  public int getSize(LongBuffer longBuffer) {
    return (int) longBuffer.position(offset + (maxItemCount * 2)).get(0);
  }

  public int putSize(LongBuffer longBuffer, int size) {
    //    return (int)
    //        longBuffer
    //            .position(offset + (maxItemCount * 2))
    //            .put(0, ((getLeftMost(longBuffer) << 32) | (long) size));
    return -1;
  }

  private long getLeftMost(LongBuffer longBuffer) {
    return (longBuffer.position(offset + (maxItemCount * 2)).get(0) >> 32);
  }

  public void put(LongBuffer longBuffer, LongBuffer temp, long key, long value) {}

  protected int binarySearch(LongBuffer longBuffer, int fromIndex, int toIndex, long key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = longBuffer.get(mid);

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
  }
}
