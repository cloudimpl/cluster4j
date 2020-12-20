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

/** @author nuwansa */
public interface ReadOnlyNode {
  public static final int LEAF = 4;
  public static final int INDEX = 8;

  long find(long key);

  LeafNode.Iterator findGe(long key);

  LeafNode.Iterator findLe(long key);

  long getKey(int index);

  long getValue(int index);

  int getSize();

  int getCapacity();

  boolean hasNext();

  boolean hasPrevious();

  int getType();

  ReadOnlyNode next();

  int getOffset();
}
