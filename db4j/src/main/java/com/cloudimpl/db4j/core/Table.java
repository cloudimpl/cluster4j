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
package com.cloudimpl.db4j.core;

import reactor.core.publisher.Flux;

/** @author nuwansa */
public class Table {
  public static void main(String[] args) throws InterruptedException {
    Flux<Integer> flux1 = Flux.fromArray(new Integer[] {4, 5, 8, 10});
    Flux<Integer> flux2 = Flux.fromArray(new Integer[] {3, 7, 8, 20});
    Flux<Integer> flux3 = Flux.fromArray(new Integer[] {1, 6, 9, 12});
    flux1
        .mergeOrderedWith(flux2, (Integer left, Integer right) -> Integer.compare(left, right))
        .mergeOrderedWith(flux3, (Integer left, Integer right) -> Integer.compare(left, right))
        .doOnNext(System.out::println)
        .subscribe();
    Thread.sleep(10000);
  }
}
