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

import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/** * @author nuwansa */
public class FluxEx {

  private FluxSink<Long> sink;
  private final Flux<Long> flux;
  private final Supplier<BPlusTree.Iterator> iteSupplier;
  private BPlusTree.Iterator ite;

  public FluxEx(Supplier<BPlusTree.Iterator> iteSuppler) {
    this.iteSupplier = iteSuppler;
    flux = Flux.fromIterable(() -> iteSuppler.get());
  }

  public Flux<Long> flux() {
    return flux;
  }

  private void set(FluxSink<Long> sink) {
    if (this.sink != null) {
      throw new RuntimeException("in used");
    }
    this.sink = sink;
    this.ite = iteSupplier.get();
    doOnRequest(1);
    sink.onRequest(this::doOnRequest);
  }

  private synchronized void doOnRequest(long req) {
    try {
      System.out.println("enter:" + Thread.currentThread().getName());
      if (this.ite == null) {
        return;
      }
      while (req > 0 && this.ite.hasNext()) {
        this.sink.next(this.ite.next());
        System.out.println("req:" + req);
        this.ite.moveToNext();
        req--;
      }

    } catch (Exception ex) {
      ex.printStackTrace();
    }
    System.out.println("exit:" + Thread.currentThread().getName());
  }
}
