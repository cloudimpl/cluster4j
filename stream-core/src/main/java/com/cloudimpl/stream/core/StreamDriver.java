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
package com.cloudimpl.stream.core;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudService;
import reactor.core.publisher.Mono;

/** @author nuwansa */
public interface StreamDriver {
  Mono<CloudService> discover(String streamName);

  Mono<CloudService> create(String streamName, StreamAttributes attr);

  Mono<CloudMessage> persist(String streamName, CloudMessage msg, StreamAttributes attr);
}
