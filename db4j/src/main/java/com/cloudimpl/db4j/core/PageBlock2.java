/**
 * Copyright 2020 nuwansa.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.db4j.core;

import java.nio.LongBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.math.MathFlux;

// ** @author nuwansa */
public class PageBlock2 {

  private final int numberOfPages;
  private final int itemSize;
  private final int pageItemCount;
  private final LongBuffer mainBuf;
  private final Page[] pages;
  private int pageIndex;
  private Page page;

  public PageBlock2(int numberOfPages, int itemSize, int pageItemCount) {
    this.numberOfPages = numberOfPages == 0 ? 1 : numberOfPages;
    this.itemSize = itemSize;
    this.pageItemCount = pageItemCount;
    this.mainBuf = LongBuffer.allocate(this.pageItemCount * this.numberOfPages);
    pages = new Page[this.numberOfPages];
    initPages();
    this.pageIndex = 0;
    this.page = pages[this.pageIndex++];
  }

  public synchronized boolean put(long key) {
    if (!this.page.put(key) && pageIndex < this.numberOfPages) {
      this.page.sort();
      this.page = pages[this.pageIndex++];
      return put(key);
    }
    return false;
  }

  public void sort() {
    this.page.sort();
  }

  public Flux<Long> find(long key) {
    return Flux.fromArray(this.pages)
        .take(this.pageIndex)
        .parallel()
        .runOn(Schedulers.parallel())
        .flatMap(p -> p.find(key))
        .sequential();
  }

  public Flux<Long> findGt(long key, int topN) {
    System.out.println("searching:" + this.pageIndex + " indexes");
    return Flux.fromArray(this.pages)
        .take(this.pageIndex)
        .filter(p -> key < p.max)
        //   .parallel()
        //  .runOn(Schedulers.parallel())
        .map(
            p -> p.findGt(key, topN == -1 ? Long.MAX_VALUE : topN)
            //  .doOnSubscribe(s -> System.out.println("start:" + p.items.arrayOffset()))
            //   .doOnTerminate(() -> System.out.println("end:" + p.items.arrayOffset()))
            )
        // .sequential()
        .collectSortedList((l1, l2) -> Integer.compare(l1.hashCode(), l2.hashCode()))
        .doOnNext(l -> System.out.println("size: " + l.size()))
        .flatMapMany(list -> Flux.<Item>mergeOrdered(list.toArray(Flux[]::new)))
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

  private void initPages() {
    int i = 0;
    while (i < this.pages.length) {
      this.pages[i] = new Page(mainBuf.slice().limit(this.pageItemCount));
      mainBuf.position(mainBuf.position() + this.pageItemCount);
      i++;
    }
  }

  @SupportedSourceVersion(SourceVersion.RELEASE_8)
  public static final class Page {

    private final LongBuffer items;
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private volatile boolean sorted = false;
    private volatile boolean readOnly = false;

    private long max = Long.MIN_VALUE;
    private long min = Long.MAX_VALUE;

    public Page(LongBuffer items) {
      this.items = items;
    }

    public boolean put(long key) {
      if (items.hasRemaining()) {
        this.items.put(key);
        this.sorted = false;
        this.max = Long.max(this.max, key);
        this.min = Long.min(this.min, key);
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

      Arrays.sort(
          this.items.array(),
          this.items.arrayOffset(),
          this.items.arrayOffset() + this.items.limit());
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

    public Flux<Long> find(long key) {
      if (readOnly) {
        return findOnReadOnly(key);
      } else {
        return findOnWritable(key);
      }
    }

    public Flux<Long> findGt(long key, long topN) {
      if (key > this.max) {
        return Flux.empty();
      }
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

    public Flux<Long> findOnReadOnly(long key) {
      this.sort();
      int idx =
          Arrays.binarySearch(
              this.items.array(),
              this.items.arrayOffset(),
              this.items.arrayOffset() + this.items.limit(),
              key);
      if (idx >= 0) {
        return Flux.fromStream(Arrays.stream(items.array()).boxed())
            .skip(idx)
            .take(this.items.position())
            .takeUntil(val -> val == key);

      } else {
        return Flux.empty();
      }
    }

    public Flux<Long> findOnWritable(long key) {
      return Flux.fromStream(Arrays.stream(items.array()).boxed())
          .skip(this.items.arrayOffset())
          .take(this.items.position())
          .filter(b -> b == key);
    }

    private Mono<Long> findMaxOnReadOnly() {
      this.sort();
      return Mono.just(this.items.get(this.items.position() - 1));
    }

    private Mono<Long> findMinOnReadOnly() {
      this.sort();
      return Mono.just(this.items.get(0));
    }

    private Mono<Long> findMaxOnWritable() {
      return MathFlux.max(Flux.fromStream(Arrays.stream(items.array()).boxed()));
    }

    private Mono<Long> findMinOnWritable() {
      return MathFlux.min(Flux.fromStream(Arrays.stream(items.array()).boxed()));
    }

    private Flux<Long> findGtOnReadOnly(long key, long topN) {
      this.sort();
      //    long start = System.nanoTime();
      int idx =
          Arrays.binarySearch(
              this.items.array(),
              this.items.arrayOffset(),
              this.items.arrayOffset() + this.items.position(),
              key);
      //     System.out.println("bs : " + (System.nanoTime() - start));
      if (idx >= 0) {

        return Flux.fromStream(Arrays.stream(items.array()).boxed())
            .skip(idx)
            .take(this.items.position() - (idx - this.items.arrayOffset()))
            .filter(l -> l != key)
            .take(topN);
      } else {
        idx = Math.abs(idx) - 1;
        int take = this.items.position() - (idx - this.items.arrayOffset());
        return Flux.fromStream(Arrays.stream(items.array()).boxed())
            .skip(idx)
            .take(take)
            .take(topN);
      }
    }

    private Flux<Long> findGtOnWritable(long key, long topN) {
      return Flux.fromStream(Arrays.stream(items.array()).boxed())
          .skip(this.items.arrayOffset())
          .take(this.items.position())
          .filter(b -> b > key)
          .sort()
          .take(topN);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    int[] arr = {1, 5, 8, 77, 333, 22222};
    System.out.println(Arrays.binarySearch(arr, 3, arr.length, 9));
    //    Flux.range(0, 10)
    //        .parallel()
    //        .runOn(Schedulers.parallel())
    //        .doOnNext(i -> System.out.println(Thread.currentThread().getName() + ":" + i))
    //        .sequential()
    //        .doOnNext(i -> System.out.println(Thread.currentThread().getName() + ":-" + i))
    //        .subscribe();
    int vol = 20_000_000;
    List<Long> list = IntStream.range(0, vol).mapToObj(i -> (long) i).collect(Collectors.toList());
    Collections.shuffle(list);

    int itemCount = 1310720;
    PageBlock2 pageBlock = new PageBlock2(vol / itemCount, 16, itemCount);
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

    long ky = 19_999_000; // list.get(r.nextInt(vol));
    Flux.interval(Duration.ofSeconds(1000000))
        .skip(1)
        .map(k -> list.get(r.nextInt(vol)))
        .doOnNext(k -> System.out.print("start searching : " + ky))
        .flatMap(k -> pageBlock.findGt(ky, 150).doOnNext(item -> System.out.println(item)))
        .sort()
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
      pageBlock.put(list.get(i));
      i++;
      rate++;
      long end = System.currentTimeMillis();
      if (end - start >= 1000) {
        System.out.println("rate: " + rate);
        rate = 0;
        start = System.currentTimeMillis();
      }
    }
    pageBlock.sort();
    AtomicLong s = new AtomicLong();
    Flux.interval(Duration.ofSeconds(1))
        .doOnSubscribe(k -> s.set(System.currentTimeMillis()))
        .take(1)
        .map(k -> list.get(r.nextInt(vol)))
        .doOnNext(k -> System.out.println("start searching : " + ky))
        .flatMap(k -> pageBlock.findGt(ky, 10))
        //  .sort((Item left, Item right) -> Long.compare(left.getKey(), right.getKey()))
        .doOnNext(k -> System.out.println("kgt:" + k))
        .doOnError(thr -> thr.printStackTrace())
        .doOnTerminate(
            () -> System.out.println("terminated:" + (System.currentTimeMillis() - s.get())))
        .subscribe();
    Thread.sleep(1000000000);
  }
}
