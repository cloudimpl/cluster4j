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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** @author nuwansa */
public class PageOld {

  public PageOld next;
  private PageOld prev;
  private final ByteBuffer buf;
  private final ByteBuffer[] bufArray;
  private int index;

  public PageOld(int itemCount) {
    buf = ByteBuffer.allocate(itemCount * 16);
    bufArray = new ByteBuffer[buf.capacity() / 16];
    init();
  }

  private void init() {
    int i = 0;
    while (i < bufArray.length) {
      bufArray[i] = buf.slice();
      buf.position(buf.position() + 16);
      i++;
    }
  }

  public boolean put(long val, long pos) {
    if (index >= bufArray.length) {
      sort(false);
      return false;
    }
    bufArray[index++].putLong(val).putLong(pos);
    return true;
  }

  public void sort(boolean log) {
    long start = 0;
    if (log) {
      start = System.currentTimeMillis();
    }
    Arrays.parallelSort(
        bufArray,
        0,
        index,
        (ByteBuffer left, ByteBuffer right) -> Long.compare(left.getLong(0), right.getLong(0)));
    if (log) {
      System.out.println("sort : " + (System.currentTimeMillis() - start));
    }
  }

  public static void main(String[] args) throws InterruptedException {
    int vol = 20_000_000;
    List<Long> list =
        IntStream.range(0, 20_000_000).mapToObj(i -> (long) i).collect(Collectors.toList());
    Collections.shuffle(list);
    int pageItemCount = 256;
    PageOld[] pages =
        IntStream.range(0, vol / 256).mapToObj(i -> new PageOld(pageItemCount)).toArray(PageOld[]::new);
    System.gc();
    System.out.println("sleeping");
    Thread.sleep(10000);

    int pageIndex = 0;
    long start = System.currentTimeMillis();
    int rate = 0;
    int i = 0;
    int pagesCount = 1;
    PageOld p = pages[pageIndex++];
    while (i < vol) {
      if (!p.put(list.get(i), i)) {
        p = pages[pageIndex++];
        p.put(list.get(i), i);
        pagesCount++;
      }
      p.sort(false);
      //      level.add(list.get(i), i);
      //      level.sort();
      rate++;
      long end = System.currentTimeMillis();
      if (end - start >= 1000) {
        System.out.println("rate: " + rate + " pages: " + pagesCount);
        rate = 0;
        start = System.currentTimeMillis();
      }
      i++;
    }
    System.out.println(" total pages: " + pagesCount);
  }
}
