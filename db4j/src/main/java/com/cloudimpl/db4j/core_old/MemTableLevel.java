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
package com.cloudimpl.db4j.core_old;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** @author nuwansa */
public class MemTableLevel implements TableLevel {

  private final Map<String, MemNumberColumnStore> mapNumColumns = new ConcurrentHashMap<>();
  private final Map<String, MemNumberColumnStore> mapStrColumns = new ConcurrentHashMap<>();
  private final String tablePath;

  public MemTableLevel(String tablePath) {
    this.tablePath = tablePath + "/level0";
  }

  public void put(String colName, BigDecimal key, long pos) {
    mapNumColumns
        .computeIfAbsent(colName, col -> new MemNumberColumnStore(col, this.tablePath))
        .add(key.longValue(), pos);
  }

  @Override
  public Flux<Long> find(String colName, char type, String value) {
    return getColumn(colName, type).flatMapMany(col -> col.find(value));
  }

  @Override
  public Flux<Long> fingGT(String colName, char type, String value) {
    return getColumn(colName, type).flatMapMany(col -> col.findGT(value));
  }

  @Override
  public Flux<Long> findGTE(String colName, char type, String value) {
    return getColumn(colName, type).flatMapMany(col -> col.findGTE(value));
  }

  @Override
  public Flux<Long> findLT(String colName, char type, String value) {
    return getColumn(colName, type).flatMapMany(col -> col.findLT(value));
  }

  @Override
  public Flux<Long> findLTE(String colName, char type, String value) {
    return getColumn(colName, type).flatMapMany(col -> col.findLTE(value));
  }

  private Mono<ColumnStore> getColumn(String colName, char type) {

    ColumnStore store = null;
    switch (type) {
      case 'S':
        {
          store = mapStrColumns.get(colName);
          break;
        }
      case 'N':
        {
          store = mapNumColumns.get(colName);
        }
    }

    if (store == null) {
      return Mono.empty();
    } else {
      return Mono.just(store);
    }
  }
}
