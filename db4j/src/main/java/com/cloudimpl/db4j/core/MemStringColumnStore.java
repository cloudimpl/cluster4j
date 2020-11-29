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
package com.cloudimpl.db4j.core;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import reactor.core.publisher.Flux;

/** @author nuwansa */
public class MemStringColumnStore extends StringColumnStore {

  private final String tablePath;
  private final DB db;
  private final BTreeMap<String, Long> map;

  public MemStringColumnStore(String name, String tablePath) {
    this.tablePath = tablePath;
    this.db =
        DBMaker.fileDB(tablePath + "/" + name + ".cidx")
            .fileMmapEnable()
            .transactionEnable()
            .make();
    this.map =
        this.db
            .treeMap("map")
            .keySerializer(Serializer.STRING)
            .valueSerializer(Serializer.LONG)
            .counterEnable()
            .createOrOpen();
  }

  public void add(String key, long value) {
    this.map.put(key, value);
  }

  public void commit() {
    this.db.commit();
  }

  @Override
  public long getSizeInBytes() {
    return 0L;
  }

  @Override
  public Flux<Long> find(String value) {
    Long pos = map.get(value);
    if (pos == null) return Flux.empty();
    else return Flux.just(pos);
  }

  @Override
  public Flux<Long> findGT(String value) {
    Long pos = map.get(value);
    return null;
  }

  @Override
  public Flux<Long> findGTE(String value) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Flux<Long> findLT(String value) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Flux<Long> findLTE(String value) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }
}
