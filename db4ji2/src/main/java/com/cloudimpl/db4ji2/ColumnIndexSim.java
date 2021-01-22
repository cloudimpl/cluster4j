/*
 * Copyright 2021 nuwansa.
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
package com.cloudimpl.db4ji2;

import com.cloudimpl.db4ji2.idx.lng.LongColumnIndex;
import com.cloudimpl.db4ji2.core.LongEntry;
import com.cloudimpl.db4ji2.core.LongComparable;
import com.cloudimpl.db4ji2.idx.lng.DirectLongMemBlockPool;
import java.util.Random;

/** @author nuwansa */
public class ColumnIndexSim extends LongColumnIndex {

  private Random r = new Random(System.currentTimeMillis());
  private long i = 0;

  public ColumnIndexSim(String colName, int memSize, int pageSize,LongComparable comparable) {
    super(colName, new DirectLongMemBlockPool(memSize, pageSize),comparable,()->new LongEntry());
  }

  public void write() {
    super.put(r.nextInt(), i++);
  }
}
