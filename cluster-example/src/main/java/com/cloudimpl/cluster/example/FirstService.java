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
package com.cloudimpl.cluster.example;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.RouterType;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
@CloudFunction(name = "FirstService")
@Router(routerType = RouterType.ROUND_ROBIN)
public class FirstService implements Function<CloudMessage, Mono<String>>{

    public FirstService(@Named("RRHnd") BiFunction<String, Object, Mono> rrHnd) throws IOException {
        DefaultExports.initialize();
        //io.prometheus.client.exporter.HTTPServer httpServer = new HTTPServer(50000,true);
        Flux.interval(Duration.ofSeconds(5))
                .flatMap(i->rrHnd.apply("GreetingService", "hello"+i).doOnNext(c->System.out.println(c)))
                .doOnError(e->((Throwable)e).printStackTrace())
                .retry(Integer.MAX_VALUE)
                .subscribe();
    }

    
    @Override
    public Mono<String> apply(CloudMessage t) {
        return Mono.just(t.data()+" world");
    }
    
}
