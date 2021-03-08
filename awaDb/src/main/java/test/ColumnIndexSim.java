package test;

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


import java.util.Random;
import org.green.jelly.MutableJsonNumber;

/** @author nuwansa */
public class ColumnIndexSim extends NumberColumnIndex {

  private Random r = new Random(System.currentTimeMillis());
  private long i = 0;
  private MutableJsonNumber json = new MutableJsonNumber();
  public ColumnIndexSim(String colName, int memSize) {
    super(colName,memSize);
  }

  public void write() {
      json.set(r.nextLong(),0);
    super.put(json, i++);
  }
}
