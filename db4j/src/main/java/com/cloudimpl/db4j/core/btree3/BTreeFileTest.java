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

import java.io.File;
import java.util.Random;
import java.util.stream.IntStream;

/** @author nuwansa */
public class BTreeFileTest {
  public static void main(String[] args) {
    int i = 0;
    int vol = 65025 * 2;
    File f = new File("/users/nuwansa/data/btree.idx");
    f.delete();
    Random r = new Random(System.currentTimeMillis());
    BTree tree = BTree.create("/users/nuwansa/data/btree.idx", vol, 4096);
    long[] arr =
        IntStream.range(0, vol)
            .mapToLong(l -> r.nextInt(vol))
            // .boxed()
            .sorted()
            .toArray();

    while (i < vol) {
      tree.put(arr[i], arr[i] * 10);
      i++;
    }
    tree.complete();

    BTreeReadOnly tree2 = BTree.from("/Users/nuwansa/data/3_353119807740782.idx");

    //  while (i < 65025)
    {
      // if (tree2.find(arr[i]) != arr[i] * 10) throw new RuntimeException("err : " + arr[i]);
      tree2.all().forEachRemaining(System.out::println);
      i++;
    }
  }
}
