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
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.math.MathFlux;

/** @author nuwansa */
public class PageBlock {

  private final int numberOfPages;
  private final int itemSize;
  private final int pageItemCount;
  private final ByteBuffer[] items;
  private final ByteBuffer mainBuf;
  private final Page[] pages;
  private int pageIndex;
  private Page page;

  public PageBlock(int numberOfPages, int itemSize, int pageItemCount) {
    this.numberOfPages = numberOfPages == 0 ? 1 : numberOfPages;
    this.itemSize = itemSize;
    this.pageItemCount = pageItemCount;
    this.items = new ByteBuffer[pageItemCount * this.numberOfPages];
    this.mainBuf =
        ByteBuffer.allocateDirect(this.itemSize * this.pageItemCount * this.numberOfPages)
            .order(ByteOrder.LITTLE_ENDIAN);
    initItems();
    pages = new Page[this.numberOfPages];
    initPages();
    this.pageIndex = 0;
    this.page = pages[this.pageIndex++];
  }

  public synchronized boolean put(long key, long value) {
    if (!this.page.put(key, value) && pageIndex < this.numberOfPages) {
      this.page.sort();
      this.page = pages[this.pageIndex++];
      return put(key, value);
    }
    return false;
  }

  public Flux<Item> find(ByteBuffer key) {
    return Flux.fromArray(this.pages)
        .take(this.pageIndex)
        .parallel()
        .runOn(Schedulers.parallel())
        .flatMap(p -> p.find(key))
        .sequential();
  }

  public Flux<Item> findGt(ByteBuffer key, int topN) {
    return Flux.fromArray(this.pages)
        .take(this.pageIndex)
        .parallel()
        .runOn(Schedulers.parallel())
        .map(p -> p.findGt(key, topN == -1 ? Long.MAX_VALUE : topN))
        // .sequential()
        .collectSortedList((l1, l2) -> Integer.compare(l1.hashCode(), l2.hashCode()))
        .flatMapMany(list -> Flux.<Item>mergeOrdered(list.toArray(new Flux[list.size()])))
        .cast(Item.class)
        .take(topN == -1 ? Long.MAX_VALUE : topN);
  }

  public Mono<Long> findMax() {
    return MathFlux.max(
        Flux.fromArray(this.pages)
            .take(this.pageIndex)
            .parallel()
            .runOn(Schedulers.parallel())
            .flatMap(p -> p.findMax())
            .sequential());
  }

  public Mono<Long> findMin() {
    return MathFlux.min(
        Flux.fromArray(this.pages)
            .take(this.pageIndex)
            .parallel()
            .runOn(Schedulers.parallel())
            .flatMap(p -> p.findMin())
            .sequential());
  }

  private void initItems() {
    int i = 0;
    while (i < this.items.length) {
      this.items[i] = this.mainBuf.slice();
      this.mainBuf.position(this.mainBuf.position() + itemSize);
      i++;
    }
  }

  private void initPages() {
    int i = 0;
    while (i < this.pages.length) {
      this.pages[i] = new Page(items, i * this.pageItemCount, (i + 1) * this.pageItemCount);
      i++;
    }
  }

  public static final class Page {

    private final ByteBuffer[] items;
    private final int limit;
    private int index;
    private final int offset;
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private volatile boolean sorted = false;
    private volatile boolean readOnly = false;

    public Page(ByteBuffer[] items, int offset, int limit) {
      this.items = items;
      this.limit = limit;
      this.offset = offset;
      this.index = offset;
    }

    public boolean put(long key, long pos) {

      if (this.index < this.limit) {
        this.items[this.index++].putLong(key).putLong(pos);
        this.sorted = false;
        // this.sort();
        return true;
      }
      setReadOnly();
      return false;
    }

    protected void setReadOnly() {
      this.readOnly = true;
    }

    private void sort() {
      if (sorted) {
        return;
      }
      // lock();
      Arrays.sort(
          items,
          this.offset,
          this.index,
          (ByteBuffer left, ByteBuffer right) -> Long.compare(left.getLong(0), right.getLong(0)));
      this.sorted = true;
      //  release();
    }

    public void lock() {
      if (readOnly) {
        return;
      }
      while (!lock.compareAndSet(false, true)) {}
    }

    public void release() {
      if (readOnly) {
        return;
      }
      this.lock.compareAndSet(true, false);
    }

    public Flux<Item> find(ByteBuffer key) {
      if (readOnly) {
        return findOnReadOnly(key);
      } else {
        return findOnWritable(key);
      }
    }

    public Flux<Item> findGt(ByteBuffer key, long topN) {
      if (readOnly) {
        return findGtOnReadOnly(key, topN);

      } else {
        return findGtOnWritable(key, topN);
      }
    }

    public Mono<Long> findMax() {
      if (readOnly) {
        return findMaxOnReadOnly();
      } else {
        return findMaxOnWritable();
      }
    }

    public Mono<Long> findMin() {
      if (readOnly) {
        return findMinOnReadOnly();
      } else {
        return findMinOnWritable();
      }
    }

    public Flux<Item> findOnReadOnly(ByteBuffer key) {
      this.sort();
      int idx =
          Arrays.binarySearch(
              this.items,
              this.offset,
              this.index,
              key,
              (ByteBuffer left, ByteBuffer right) ->
                  Long.compare(left.getLong(0), right.getLong(0)));
      if (idx >= 0) {
        return Flux.fromArray(items)
            .skip(idx)
            .take(this.index - idx)
            .takeUntil(buf -> buf.getLong(0) == key.getLong(0))
            .map(buf -> new Item(buf));

      } else {
        return Flux.empty();
      }
    }

    public Flux<Item> findOnWritable(ByteBuffer key) {
      return Flux.fromArray(items)
          .skip(this.offset)
          .take(this.index - this.offset)
          .filter(b -> b.getLong(0) == key.getLong(0))
          .map(buf -> new Item(buf));
    }

    private Mono<Long> findMaxOnReadOnly() {
      this.sort();
      ByteBuffer val = this.items[this.limit - 1];
      return Mono.just(val.getLong(0));
    }

    private Mono<Long> findMinOnReadOnly() {
      this.sort();
      ByteBuffer val = this.items[this.offset];
      return Mono.just(val.getLong(0));
    }

    private Mono<Long> findMaxOnWritable() {
      return MathFlux.max(
          Flux.<Long>fromStream(
              IntStream.range(this.offset, this.index).mapToObj(i -> this.items[i].getLong(0))));
    }

    private Mono<Long> findMinOnWritable() {
      return MathFlux.min(
          Flux.<Long>fromStream(
              IntStream.range(this.offset, this.index).mapToObj(i -> this.items[i].getLong(0))));
    }

    private Flux<Item> findGtOnReadOnly(ByteBuffer key, long topN) {
      this.sort();
      //    long start = System.nanoTime();
      int idx =
          Arrays.binarySearch(
              this.items,
              this.offset,
              this.index,
              key,
              (ByteBuffer left, ByteBuffer right) ->
                  Long.compare(left.getLong(0), right.getLong(0)));
      //     System.out.println("bs : " + (System.nanoTime() - start));
      if (idx >= 0) {

        return Flux.fromArray(items)
            .skip(idx)
            .take(this.index - idx)
            .filter(buf -> buf.getLong(0) != key.getLong(0))
            .map(buf -> new Item(buf))
            .take(topN);
      } else {
        idx = Math.abs(idx) - 1;
        return Flux.fromArray(items)
            .skip(idx)
            .take(this.index - idx)
            .map(buf -> new Item(buf))
            .take(topN);
      }
    }

    private Flux<Item> findGtOnWritable(ByteBuffer key, long topN) {
      return Flux.fromArray(items)
          .skip(this.offset)
          .take(this.index - this.offset)
          .filter(b -> b.getLong(0) > key.getLong(0))
          .map(buf -> new Item(buf))
          .sort()
          .take(topN);
    }
  }

  public static void main(String[] args) throws InterruptedException {

    LongBuffer buf = LongBuffer.allocate(64);
    buf.put(100);
    LongBuffer longBuf = buf.slice();
    buf.position(8);
    LongBuffer longBuf2 = buf.slice();
    System.out.println(longBuf2.array()[0]);
    int[] arr = {1, 5, 8, 77, 333, 22222};
    System.out.println(Arrays.binarySearch(arr, 3, arr.length, 9));
    //    Flux.range(0, 10)
    //        .parallel()
    //        .runOn(Schedulers.parallel())
    //        .doOnNext(i -> System.out.println(Thread.currentThread().getName() + ":" + i))
    //        .sequential()
    //        .doOnNext(i -> System.out.println(Thread.currentThread().getName() + ":-" + i))
    //        .subscribe();
    int vol = 20_000_00;
    List<Long> list = IntStream.range(0, vol).mapToObj(i -> (long) i).collect(Collectors.toList());
    // Collections.shuffle(list);

    PageBlock pageBlock = new PageBlock(vol / 256, 16, 256);
    System.gc();
    System.out.println("sleeping");
    int i = 0;
    // Thread.sleep(10000);
    System.out.println("searching : " + list.get(0));
    Random r = new Random(System.currentTimeMillis());
    //    Flux.interval(Duration.ofSeconds(1))
    //        .skip(1)
    //        .doOnNext(k -> System.out.print("start searching"))
    //        .flatMap(k ->
    // pageBlock.find(ByteBuffer.allocate(8).putLong(list.get(r.nextInt(vol)))))
    //        .doOnNext(k -> System.out.println("k:" + k))
    //        .doOnError(thr -> thr.printStackTrace())
    //        .subscribe();

    long ky = 0; // list.get(r.nextInt(vol));
    Flux.interval(Duration.ofSeconds(1000000))
        .skip(1)
        .map(k -> list.get(r.nextInt(vol)))
        .doOnNext(k -> System.out.print("start searching : " + ky))
        .flatMap(
            k ->
                pageBlock
                    .findGt(ByteBuffer.allocate(8).putLong(ky), 150)
                    .doOnNext(item -> System.out.println(item.getKey() + ":" + item.getValue())))
        .sort((Item left, Item right) -> Long.compare(left.getKey(), right.getKey()))
        .take(10)
        .doOnNext(k -> System.out.println("kgt:" + k))
        .doOnError(thr -> thr.printStackTrace())
        .subscribe();
    //    Flux.interval(Duration.ofSeconds(1))
    //        .doOnNext(k -> System.out.print("start searching"))
    //        .flatMap(k -> pageBlock.findMax())
    //        .doOnNext(k -> System.out.println("x:" + k))
    //        .doOnError(thr -> thr.printStackTrace())
    //        .subscribe();
    //
    //    Flux.interval(Duration.ofSeconds(1))
    //        .doOnNext(k -> System.out.print("start searching"))
    //        .flatMap(k -> pageBlock.findMin())
    //        .doOnNext(k -> System.out.println("y:" + k))
    //        .doOnError(thr -> thr.printStackTrace())
    //        .subscribe();

    int rate = 0;
    long start = System.currentTimeMillis();
    while (i < vol) {
      pageBlock.put(list.get(i), i);
      i++;
      rate++;
      long end = System.currentTimeMillis();
      if (end - start >= 1000) {
        System.out.println("rate: " + rate);
        rate = 0;
        start = System.currentTimeMillis();
      }
    }
    long s = System.currentTimeMillis();
    Flux.interval(Duration.ofSeconds(1))
        .take(1)
        .map(k -> list.get(r.nextInt(vol)))
        .doOnNext(k -> System.out.print("start searching : " + ky))
        .flatMap(k -> pageBlock.findGt(ByteBuffer.allocate(8).putLong(ky), 10))
        //  .sort((Item left, Item right) -> Long.compare(left.getKey(), right.getKey()))
        .doOnNext(k -> System.out.println("kgt:" + k))
        .doOnError(thr -> thr.printStackTrace())
        .doOnTerminate(() -> System.out.println("terminated:" + (System.currentTimeMillis() - s)))
        .subscribe();
    Thread.sleep(1000000000);
  }
}
