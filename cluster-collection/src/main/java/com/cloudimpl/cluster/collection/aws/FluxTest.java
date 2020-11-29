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
package com.cloudimpl.cluster.collection.aws;

import java.time.Duration;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class FluxTest {
    public static void main(String[] args) throws InterruptedException {
        Flux<String> flux1 = Flux.interval(Duration.ofSeconds(1)).doOnNext(i->{
            if(i > 0 && i % 5 == 0)
                throw new RuntimeException("xxx");
        }).map(i->"flux1"+i);
        
        Flux<String> flux2 = Flux.interval(Duration.ofSeconds(1)).doOnNext(i->{
            if(i > 0  && i % 10 == 0)
                throw new RuntimeException("yyy");
        }).map(i->"flux2"+i);
        
        Flux<String>[] fluxArr = new Flux[]{flux1,flux2};
        Flux.fromArray(new Integer[]{0,1}).flatMap(i->fluxArr[i].onErrorResume(thr->Flux.empty())).doOnNext(System.out::println).doOnError(thr->thr.printStackTrace())
                .doOnTerminate(()->System.out.println("terminated"))
                .subscribe();
        Thread.sleep(100000000);
    }
}
