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
package com.cloudimpl.db4j.core.btree2;

import btree4j.BTreeException;
import com.cloudimpl.db4j.core.btree3.SortedIterator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/** @author nuwansa */
public class Test {
  public static void main(String[] args) throws BTreeException {
    //    File tmpDir = FileUtils.getTempDir();
    //    File tmpFile = new File(tmpDir, "JMHBenchmark.idx");
    //    tmpFile.deleteOnExit();
    //    if (tmpFile.exists()) {
    //      //    Assert.assertTrue(tmpFile.delete());
    //    }
    //    BTreeIndex btree = new BTreeIndex(tmpFile);
    //    btree.init(/* bulkload */ false);

    //    Long v = 7L; // (1L & 0xFFL) << 63;
    //    System.out.println(Long.toBinaryString(v) + " :" + Long.toBinaryString(v).length());
    //    System.out.println("v:" + ((v & 0x000000FFL >> 63) & 0xFFL));
    //    long k = v >> 1;
    //    System.out.println("k:" + k + ":" + v);
    //    System.out.println(Long.toBinaryString(k & 0x1));
    // btree.search(new IndexQuery, callback);

    long v = 1 << 8;
    v = v | 1;
    System.out.println("v:" + ((v & 0xFFFFFFFFFFFFFF00L) | 2));
    System.out.println(Long.toBinaryString(0xFFFFFFFFFFFFFF00L));

    int[] arr = new int[10];

    arr[0] = 1;
    arr[1] = 5;
    arr[2] = 7;
    arr[3] = 8;

    System.arraycopy(arr, 0, arr, 1, 4);
    // arr[1] = 0;

    System.out.println(Arrays.toString(arr));
    int pos = Arrays.binarySearch(arr, 10);
    System.out.println("pos:" + pos);

    List<Integer> list1 = Arrays.asList(1, 5, 8, 9);
    List<Integer> list2 = Arrays.asList(-1, 5, 7, 10);

    Iterator<Integer> it = new SortedIterator(list1.iterator(), list2.iterator());
    it.forEachRemaining(System.out::println);
  }
}
