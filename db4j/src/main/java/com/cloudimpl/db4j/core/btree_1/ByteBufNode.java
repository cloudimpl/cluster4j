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
package com.cloudimpl.db4j.core.btree_1;

import java.nio.LongBuffer;
import java.util.Arrays;

// |k1|k2|k3|k4|k5|v1|v2|v3|v4|v5|n|p
/** @author nuwansa */
public class ByteBufNode implements Node {

  protected final LongBuffer buffer;

  public ByteBufNode(LongBuffer buffer) {
    this.buffer = buffer;
  }

  public long findChild(long key) {
    int pos =
        Arrays.binarySearch(
            buffer.array(), buffer.arrayOffset(), buffer.arrayOffset() + getSize(), key);
    if (pos >= 0) {
      pos = pos - buffer.arrayOffset();
      return getRight(pos);
    } else {
      pos = (-pos - 1) - buffer.arrayOffset();
      return getLeft(pos);
    }
  }

  public long find(long key) {
    int size = getSize();
    int pos =
        Arrays.binarySearch(buffer.array(), buffer.arrayOffset(), buffer.arrayOffset() + size, key);
    if (pos >= 0) {
      pos = pos - buffer.arrayOffset();
      return getLeft(pos);
    } else {
      return -1;
    }
  }

  public int findGe(long key) {
    int size = getSize();
    int pos =
        Arrays.binarySearch(buffer.array(), buffer.arrayOffset(), buffer.arrayOffset() + size, key);
    if (pos >= 0) {
      return pos - this.buffer.arrayOffset();
    } else {
      pos = -pos - 1;
      return pos - this.buffer.arrayOffset();
    }
  }

  @Override
  public long getLeft(int index) {
    return this.buffer.get(getKeyCapacity() + index);
  }

  @Override
  public long getRight(int index) {
    return this.buffer.get(getKeyCapacity() + (index + 1));
  }

  @Override
  public long getKey(int index) {
    return this.buffer.get(index);
  }

  @Override
  public int getNext() {
    return (int) (this.buffer.get(this.buffer.limit() - 1) & 0xFFFFFFFF);
  }

  @Override
  public int getPrevious() {
    return (int) (this.buffer.get(this.buffer.limit() - 1) >> 32);
  }

  protected int getFlagPos() {
    return this.buffer.limit() - 1;
  }

  protected int getKeyCapacity() {
    return (this.buffer.limit() - 3) / 2;
  }

  public static int getRequiredCapacity(int itemCount) {
    return itemCount * 2 + 3;
  }

  @Override
  public int getSize() {
    return (int) this.buffer.get(this.buffer.limit() - 2);
  }

  @Override
  public boolean isFull() {
    return getSize() == getKeyCapacity();
  }

  public Iterator iterator() {
    return new Iterator(this);
  }

  public Iterator iterator(int index) {
    return new Iterator(this, index);
  }

  public int getId() {
    return this.buffer.arrayOffset();
  }

  @Override
  public String toString() {
    String s = "(" + getId() + ")" + "[";
    Iterator ite = iterator();
    while (ite.hasNext()) {
      s += ite.getLeft() + "," + "(" + ite.getKey() + ")";
      ite.moveToNext();
      if (!ite.hasNext()) {
        s += "," + ite.getLeft();
      } else {
        s += ",";
      }
    }
    s += "](" + getSize() + ")" + "(" + getNext() + ")";
    return s;
  }

  public static final class Iterator {

    private ByteBufNode node;
    private int index;

    public Iterator(ByteBufNode node) {
      this(node, 0);
    }

    public Iterator(ByteBufNode node, int index) {
      this.node = node;
      this.index = index;
    }

    public boolean hasNext() {
      return node != null && index < node.getSize();
    }

    public void moveToNext() {
      index++;
    }

    public Long getKey() {
      return this.node.getKey(index);
    }

    public Long getLeft() {
      return this.node.getLeft(index);
    }

    public Long getRight() {
      return this.node.getRight(index);
    }
  }

  public static final class Writer {

    public static void write(ByteBufNode node, int index, long key, long value) {
      node.buffer.put(index, key);
      node.buffer.put(((node.buffer.limit() - 3) / 2) + index, value);
    }

    public static void link(ByteBufNode left, ByteBufNode right) {
      left.buffer.put(
          left.getFlagPos(),
          (((long) left.getPrevious()) << 32) | (right.buffer.arrayOffset() & 0xFFFFFFFFL));
      right.buffer.put(
          right.getFlagPos(),
          (((long) left.buffer.arrayOffset()) << 32) | (right.getNext() & 0xFFFFFFFFL));
    }

    public static void updateSize(ByteBufNode node, int size) {
      node.buffer.put(node.buffer.limit() - 2, size);
    }
  }

  public static void main(String[] args) {
    int[] aa = {1, 3, 5, 7, 9};

    System.out.println(Arrays.binarySearch(aa, 6));
    LongBuffer buf = LongBuffer.allocate(18);
    ByteBufNode node1 = new ByteBufNode(buf.slice().limit(9));
    ByteBufNode.Writer.write(node1, 0, 1, 100);
    ByteBufNode.Writer.write(node1, 1, 2, 200);
    ByteBufNode.Writer.write(node1, 2, 3, 300);

    ByteBufNode node2 = new ByteBufNode(buf.position(9).slice().limit(9));
    ByteBufNode.Writer.write(node2, 0, 4, 400);
    ByteBufNode.Writer.write(node2, 1, 5, 500);
    ByteBufNode.Writer.write(node2, 2, 6, 600);

    ByteBufNode.Writer.updateSize(node1, 3);
    ByteBufNode.Writer.updateSize(node2, 1);

    System.out.println(node1.getLeft(0) + ":" + node1.getKey(0) + ":" + node1.getRight(0));
    System.out.println(node1.getLeft(1) + ":" + node1.getKey(1) + ":" + node1.getRight(1));
    System.out.println(node1.getLeft(2) + ":" + node1.getKey(2) + ":" + node1.getRight(2));

    System.out.println(node2.getLeft(0) + ":" + node2.getKey(0) + ":" + node2.getRight(0));
    System.out.println(node2.getLeft(1) + ":" + node2.getKey(1) + ":" + node2.getRight(1));
    System.out.println(node2.getLeft(2) + ":" + node2.getKey(2) + ":" + node2.getRight(2));

    ByteBufNode.Writer.link(node1, node2);

    System.out.println("getSize: " + node1.getSize());
    System.out.println("next: " + node1.getNext() + " prev: " + node2.getPrevious());
    System.out.println("next: " + node2.getNext() + " prev: " + node1.getPrevious());
    ByteBufNode.Iterator ite = new ByteBufNode.Iterator(node1);
    while (ite.hasNext()) {
      System.out.println("key:" + ite.getKey() + " value: " + ite.getLeft());
      ite.moveToNext();
    }

    System.out.println("node count : " + Math.ceil(Math.log(2) / Math.log(10)));
  }
}
