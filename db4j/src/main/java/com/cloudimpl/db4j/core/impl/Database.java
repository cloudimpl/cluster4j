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
package com.cloudimpl.db4j.core.impl;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/** @author nuwansa */
public class Database {

  void put(Object data) {}

  public static void main(String[] args) {
    ByteBuffer buf = ByteBuffer.allocateDirect(1024);
    LongBuffer buf2 = buf.asLongBuffer();
    buf2.put(5);
    LongBuffer buf3 = buf2.slice();
    buf2.flip();
    buf3.put(buf2);
    // buf2.flip();
    buf2 = buf.asLongBuffer();
    System.out.println(buf2.get(0) + " :" + buf2.get(1));
  }
}
