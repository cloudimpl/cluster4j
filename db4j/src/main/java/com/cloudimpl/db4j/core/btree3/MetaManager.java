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
package com.cloudimpl.db4j.core.btree3;

import java.util.Map;

/** @author nuwansa */
public class MetaManager {
  private final String collectionName;
  private final Map<String, Integer> fieldToIdx;

  public MetaManager(String collectionName, Map<String, Integer> fieldToIdx) {
    this.collectionName = collectionName;
    this.fieldToIdx = fieldToIdx;
  }

  public void put(String field, int index) {}
}
