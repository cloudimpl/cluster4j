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
package com.cloudimpl.db4j.compute;

import com.cloudimpl.cluster4j.common.RouterType;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import com.cloudimpl.db4j.compute.msg.IngestPayloadRequest;
import com.cloudimpl.db4j.compute.msg.IngestResponse;
import java.util.function.BiFunction;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/** @author nuwansa */
@CloudFunction(name = "IngestService")
@Router(routerType = RouterType.ROUND_ROBIN)
public class IngestService implements Function<IngestPayloadRequest, Mono<IngestResponse>> {

  @Inject
  @Named(value = "RRHnd")
  BiFunction<String, Object, Mono<IngestResponse>> rrHnd;

  public static final String STORAGE_SERVICE_NAME = "storageService";

  @Override
  public Mono<IngestResponse> apply(IngestPayloadRequest req) {
    return rrHnd.apply(STORAGE_SERVICE_NAME, req).retry(3);
  }
}
