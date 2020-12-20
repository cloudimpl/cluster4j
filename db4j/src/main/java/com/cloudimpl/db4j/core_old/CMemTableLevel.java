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
package com.cloudimpl.db4j.core_old;

import com.cloudimpl.cluster4j.common.Pair;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** @author nuwansa */
public class CMemTableLevel {

  public static final int[] MULTI = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000};

  private final int pageSize;
  private final ByteBuffer buf;
  private final ByteBuffer[] buffers;
  private int currentIndex = 0;

  public CMemTableLevel(int pageSize) {
    this.pageSize = pageSize;
    this.buf = ByteBuffer.allocate(pageSize);
    this.buffers = new ByteBuffer[pageSize / 22];
  }

  public boolean add(BigDecimal key, long pos) {
    long wholeVal = key.longValue();
    int fraction =
        key.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(key.scale())).intValue();
    ByteBuffer currentBuf = getBuf();
    if (currentBuf == null) {
      return false;
    } else {
      currentBuf.putLong(wholeVal);
      currentBuf.putInt(fraction);
      currentBuf.putShort((short) key.scale());
      currentBuf.putLong(pos);
      return true;
    }
  }

  public void sort() {
    Arrays.sort(
        buffers,
        0,
        currentIndex,
        (ByteBuffer o1, ByteBuffer o2) -> {
          //          return BigDecimal.valueOf(o1.getLong(0), o1.getInt(8))
          //              .compareTo(BigDecimal.valueOf(o2.getLong(0), o2.getInt(8)));
          int ret = Long.compare(o1.getLong(0), o2.getLong(0));
          return ret;
          //          if (ret == 0) {
          //            int min = Math.min(o1.getShort(12), o2.getShort(12));
          //            int left = o1.getInt(8) * MULTI[o1.getShort(12) - min];
          //            int right = o2.getInt(8) * MULTI[o2.getShort(12) - min];
          //            ret = Integer.compare(left, right);
          //          }
          //          return ret;
        });
  }

  private ByteBuffer getBuf() {
    ByteBuffer ret = null;
    if (currentIndex < buffers.length) {
      ret = buffers[currentIndex] = this.buf.slice();
      this.buf.position(this.buf.position() + 22);
      this.currentIndex++;
    }
    return ret;
  }

  public static void main(String[] args) {
    CMemTableLevel level = new CMemTableLevel(1_000_00 * 22);
    List<BigDecimal> list =
        IntStream.range(0, 10_000_000)
            .mapToObj(i -> new BigDecimal(i))
            .collect(Collectors.toList());
    Collections.shuffle(list);

    Map<Pair<BigDecimal, Long>, Long> skipList =
        new ConcurrentSkipListMap<>(
            (o1, o2) -> {
              int ret = o1.getKey().compareTo(o2.getKey());

              return ret;
            });
    long start = System.currentTimeMillis();
    int rate = 0;
    int i = 0;
    while (i < list.size()) {
      skipList.put(new Pair<>(list.get(i), (long) i), (long) i);
      //      level.add(list.get(i), i);
      //      level.sort();
      rate++;
      long end = System.currentTimeMillis();
      if (end - start >= 1000) {
        System.out.println("rate: " + rate);
        rate = 0;
        start = System.currentTimeMillis();
      }
      i++;
    }
    start = System.currentTimeMillis();
    level.sort();
    System.out.println("sort : " + (System.currentTimeMillis() - start));
  }
}
