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
package com.cloudimpl.db4ji2.idx.str;

import com.cloudimpl.db4ji2.core.old.MergeItem;
import java.util.List;
import reactor.core.publisher.Mono;

/**
 * * @author nuwansa
 *
 * @param <T>
 * @param <U>
 */
public abstract class StringCompactionWorker<U> {
  private final int level;
  private final StringColumnIndex idx;

  public StringCompactionWorker(int level, StringColumnIndex idx) {
    this.level = level;
    this.idx = idx;
  }

  public int getLevel() {
    return level;
  }

  public StringColumnIndex getIdx() {
    return idx;
  }

  public abstract int getBatchCount();

  public abstract Mono<MergeItem<U>> compact(List<MergeItem<? extends StringQueryable>> items);
}
