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
package com.cloudimpl.stream.service;

import com.cloudimpl.cluster.collection.CollectionOptions;
import com.cloudimpl.cluster.collection.CollectionProvider;
import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.Named;
import java.util.NavigableMap;
import reactor.core.publisher.Flux;

/** @author nuwansa */
public class StreamServiceLoadBalancer {

  private final NavigableMap<String, StreamDetail> streamIndex;

  @Inject
  public StreamServiceLoadBalancer(
      CollectionProvider collectionProvider,
      @Named("@serviceFlux") Flux<FluxStream.Event<String, CloudService>> serviceFlux) {
    streamIndex =
        collectionProvider.createNavigableMap(
            "StreamIndex",
            CollectionOptions.builder().withOption("TableName", "StreamIndex").build());
  }
}
