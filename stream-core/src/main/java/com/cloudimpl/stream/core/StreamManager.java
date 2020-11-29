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
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/** @author nuwansa */
public class StreamManager {

  private final StreamDriver driver;

  public StreamManager(StreamDriver driver) {
    this.driver = driver;
  }

  public Flux<CloudMessage> connect(String streamName, StreamRequest req) {
    return driver
        .discover(streamName)
        .flatMapMany(
            service ->
                service.requestStream(
                    CloudMessage.builder()
                        .withData(req)
                        .withAttr(StreamHeaders.STREAM_MAME, streamName)
                        .build()));
  }

  public <T> FluxSink<T> create(String stream, StreamAttributes attr) {
    //      FluxSink<T> sink =
    //      return driver.create(stream, attr)
    return null;
  }
}
