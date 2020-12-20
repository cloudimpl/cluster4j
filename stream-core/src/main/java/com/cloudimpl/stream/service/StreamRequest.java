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

import java.util.List;
import java.util.Set;

/** @author nuwansa */
public class StreamRequest {
  private boolean create;
  private String streamName;
  private Set<String> nodeSelectors;
  private boolean persisted;
  private List<String> partitionBy;
}

/*
createOrReplace stream Test from source endpoint tcp with port = 12345 , persisted = true;
createOrReplace stream Test2 partition by userId,instId from source stream (select a , b , c from Test where a = 'qewe' and b > 3);
deploy endpoint abc  with port = 12345;
*/