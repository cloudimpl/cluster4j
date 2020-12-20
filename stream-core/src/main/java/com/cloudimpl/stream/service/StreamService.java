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
import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.RouterType;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import java.util.NavigableMap;
import java.util.function.Function;

/** @author nuwansa */
@CloudFunction(name = "StreamService")
@Router(routerType = RouterType.LEADER)
public class StreamService implements Function<CloudMessage, CloudMessage> {

  private final CollectionProvider collectionProvider;
  private final NavigableMap<String, StreamDetail> streamDetails;

  @Inject
  public StreamService(CollectionProvider collectionProvider) {
    this.collectionProvider = collectionProvider;
    this.streamDetails =
        collectionProvider.createNavigableMap(
            "StreamIndex",
            CollectionOptions.builder().withOption("TableName", "StreamIndexTable").build());
  }

  @Override
  public CloudMessage apply(CloudMessage t) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }
}
