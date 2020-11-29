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

import reactor.core.publisher.Flux;

/** @author nuwansa */
public interface TableLevel {

  public abstract Flux<Long> find(String colName, char type, String value);

  public abstract Flux<Long> fingGT(String colName, char type, String value);

  public abstract Flux<Long> findGTE(String colName, char type, String value);

  public abstract Flux<Long> findLT(String colName, char type, String value);

  public abstract Flux<Long> findLTE(String colName, char type, String value);
}
