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

import com.google.common.util.concurrent.RateLimiter;

/** @author nuwansa */
public class DatabaseSim {

  private final ColumnIndexSim[] columns;

  public DatabaseSim(int size) {
    columns = new ColumnIndexSim[size];
    int i = 0;
    while (i < size) {
      columns[i] = new ColumnIndexSim("" + i, 4096 * 1280, 4096);
      i++;
    }
  }

  public void write() {
    int i = 0;
    while (i < columns.length) {
      columns[i].write();
      i++;
    }
  }

  public static void main(String[] args) {

    DatabaseSim sim = new DatabaseSim(100);
    long i = 0;
    long size = 10_000_000;
    int rate = 0;
    long start = System.currentTimeMillis();
    long lasKey = 0;
    long firstKey = 0;
    RateLimiter limiter = RateLimiter.create(40000);
    while (i < size) {
      limiter.acquire();

      sim.write();
      if (i % 100000 == 0) {
        // System.out.println("query : " + idx.getQueryBlockCount());
        System.gc();
      }
      rate++;
      long end = System.currentTimeMillis();
      if (end - start >= 1000) {
        System.out.println(
            "rate-----------------------------------: "
                + rate
                + " : "
                // + idx.getQueryBlockCount()
                + " current : "
                + (i));
        rate = 0;
        start = System.currentTimeMillis();
      }
      i++;
    }
  }
}
