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
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import io.prometheus.client.Counter;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
@CloudFunction(name = "GreetingService")
@Router(routerType = RouterType.ROUND_ROBIN)
public class GreetingService implements Function<CloudMessage, Mono<String>>{

    
    long id = System.currentTimeMillis();
    Counter requests = Counter.build()
     .namespace("greetingservice")
     .name("req_seconds").help("Total requests.").register();
    @Override
    public Mono<String> apply(CloudMessage t) {
        requests.inc();
        return Mono.just(t.data()+" world "+requests.get());
    }
    
}
