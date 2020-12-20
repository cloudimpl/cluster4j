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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

/** @author nuwansa */
public class MMapTest {

  private static MappedByteBuffer getByteBuf(
      RandomAccessFile file, FileChannel.MapMode mapMode, int offset, int size) {
    try {
      return file.getChannel().map(mapMode, offset, size);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) throws FileNotFoundException, InterruptedException {
    RandomAccessFile file = new RandomAccessFile("/users/nuwansa/data/test.idx", "rw");
    int i = 0;
    Thread.sleep(10000);
    List<MappedByteBuffer> list = new LinkedList<>();
    while (i < 100000) {
      MappedByteBuffer buf =
          getByteBuf(file, FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024);
      //     System.out.println(buf.arrayOffset());
      list.add(buf);
      i++;
    }
  }
}
