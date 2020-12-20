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
package com.cloudimpl.db4j.core.btree_1;

import java.util.Iterator;
import reactor.core.publisher.Flux;

/** @author nuwansa */
public class BtreeTest {

  public static void main(String[] args) throws InterruptedException {
    Iterable<Integer> ite =
        () ->
            new Iterator<Integer>() {
              int i = 0;

              @Override
              public boolean hasNext() {
                return true;
              }

              @Override
              public Integer next() {
                System.out.println("xxx:" + i);
                return i++;
              }
            };

    Flux.fromIterable(ite).doOnNext(System.out::println).take(10).subscribe();
    //    List<Integer> list = Arrays.asList(IntStream.range(1,
    // 10000).boxed().toArray(Integer[]::new));
    //    Flux<Integer> flux =
    //        Flux.create(
    //            em -> {
    //              Iterator<Integer> ite = list.iterator();
    //              em.onRequest(
    //                  l -> {
    //                    System.out.println("onreq: " + l);
    //                    while (l > 0) {
    //                      if (ite.hasNext()) {
    //                        em.next(ite.next());
    //                      } else {
    //                        em.complete();
    //                      }
    //                      l--;
    //                    }
    //                  });
    //            },
    //            FluxSink.OverflowStrategy.BUFFER);
    //
    //    flux.doOnNext(System.out::println)
    //        .subscribe(
    //            new Subscriber<Integer>() {
    //              private Subscription s;
    //
    //              @Override
    //              public void onSubscribe(Subscription s) {
    //                this.s = s;
    //                this.s.request(1);
    //              }
    //
    //              @Override
    //              public void onNext(Integer t) {
    //                s.request(1);
    //              }
    //
    //              @Override
    //              public void onError(Throwable thrwbl) {}
    //
    //              @Override
    //              public void onComplete() {}
    //            });
    Thread.sleep(1000000);
  }
}
