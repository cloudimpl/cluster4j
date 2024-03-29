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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;

/** @author nuwansa */
public class BTree extends BTreeReadOnly {

  private WritableLeafNode rootLeafNode;
  private WritableLeafNode currentLeafNode;
  private int currentIndex;
  private int size;
  private BTree.Header header;

  public BTree(
      int maxItemCount,
      int pageSize,
      BTree.Header header,
      Function<Integer, ByteBuffer> bufferProvider) {
    super(header, bufferProvider);
    this.header = header;
    this.maxItemCount = maxItemCount;
    this.pageSize = pageSize;
  }

  public static BTree create(String fileName, int maxItemCount, int pageSize) {
    try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
      BTree tree =
          new BTree(
              maxItemCount,
              pageSize,
              new BTree.Header(getByteBuf(file, FileChannel.MapMode.READ_WRITE, 0, 1024)),
              size -> getByteBuf(file, FileChannel.MapMode.READ_WRITE, 1024, size));
      tree.init();
      return tree;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static BTree create(int maxItemCount, int pageSize) {
    BTree tree =
        new BTree(
            maxItemCount,
            pageSize,
            new BTree.Header(ByteBuffer.allocate(1024)),
            size -> ByteBuffer.allocate(size));
    tree.init();
    return tree;
  }

  public static BTreeReadOnly from(String fileName) {
    try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
      BTreeReadOnly tree =
          new BTreeReadOnly(
              new BTree.Header(getByteBuf(file, FileChannel.MapMode.READ_ONLY, 0, 1024)),
              size -> getByteBuf(file, FileChannel.MapMode.READ_ONLY, 1024, size));
      tree.init();
      return tree;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static MappedByteBuffer getByteBuf(
      RandomAccessFile file, FileChannel.MapMode mapMode, int offset, int size) {
    try {
      return file.getChannel().map(mapMode, offset, size);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected void init() {
    this.maxItemPerNode = getMaxItemPerNode(pageSize);
    this.leafNodeCounts = getLeafNodeCount(maxItemCount, maxItemPerNode);
    this.indexNodeCount = getTotalIndexNodeCount(this.leafNodeCounts, maxItemPerNode);
    this.mainBuffer = bufferProvider.apply(pageSize * (this.leafNodeCounts + this.indexNodeCount));
    this.rootLeafNode =
        new WritableLeafNode(mainBuffer, pageSize * indexNodeCount, maxItemPerNode, pageSize);
    this.currentLeafNode = this.rootLeafNode;
    this.currentIndex = 0;
  }

  public boolean put(long key, long value) {
    if (this.size >= this.maxItemCount) {
      return false;
    }
    if (currentIndex >= maxItemPerNode) {
      this.currentLeafNode.setSize(maxItemPerNode);
      this.currentLeafNode = this.currentLeafNode.createNext();
      this.currentLeafNode.setPrevious();
      this.currentIndex = 0;
    }

    this.currentLeafNode.setKey(this.currentIndex, key);
    this.currentLeafNode.setValue(this.currentIndex, value);
    this.currentIndex++;
    this.size++;
    return true;
  }

  @Override
  public int getSize() {
    return size;
  }

  public void complete() {
    System.out.println("complete:" + maxItemCount + ":" + leafNodeCounts);
    this.currentLeafNode.setSize(this.currentIndex);
    this.rootNode = fillLevel(this.rootLeafNode, leafNodeCounts);
    this.header.putRootOffset(this.rootNode.getOffset());
    this.header.putPageSize(this.pageSize);
    this.header.putSize(this.size);
    this.header.putMaxItemCount(this.maxItemCount);
    this.header.complete();
  }

  private ReadOnlyNode fillLevel(ReadOnlyNode node, int nodeCount) {
    // System.out.println("level started");
    int levelNodeCount = getIndexNodeCount(nodeCount, maxItemPerNode);
    WritableIndexNode firstNode =
        new WritableIndexNode(
            mainBuffer, node.getOffset() - (levelNodeCount * pageSize), maxItemPerNode, pageSize);

    //  System.out.println("node created");
    WritableIndexNode currentIndexNode = firstNode;
    NodeIterator ite = nodeIterator(node.next());
    ReadOnlyNode currentLeaf = node;
    int i = 0;
    while (ite.hasNext()) {
      i = 0;
      while (i < maxItemPerNode) {
        ReadOnlyNode next = ite.next();
        currentIndexNode.setKey(i, next.getKey(0));
        currentIndexNode.setValue(i, currentLeaf.getOffset());
        //        System.out.println(
        //            "key added:" + currentIndexNode.getKey(i) + " value:" +
        // currentIndexNode.getValue(i));
        currentLeaf = next;
        i++;
        if (!ite.hasNext()) {
          break;
        }
      }
      currentIndexNode.setSize(i);
      currentIndexNode.setValue(i, currentLeaf.getOffset());
      //      System.out.println(
      //          "key added:"
      //              + currentIndexNode.getKey(i - 1)
      //              + " right value:"
      //              + currentIndexNode.getValue(i));
      //      System.out.println("node done");
      if (ite.hasNext()) {
        currentIndexNode = currentIndexNode.createNext();
      }
    }
    currentIndexNode.setValue(i, currentLeaf.getOffset());
    if (levelNodeCount == 1) {
      return firstNode;
    }
    return fillLevel(firstNode, levelNodeCount);
  }

  public static final class Header extends BTreeReadOnly.Header {

    public Header(ByteBuffer buffer) {
      super(buffer);
    }

    public void putRootOffset(int offset) {
      this.buffer.putInt(0, offset);
    }

    public void putMaxItemCount(int itemCount) {
      this.buffer.putInt(4, itemCount);
    }

    public void putPageSize(int pageSize) {
      this.buffer.putInt(8, pageSize);
    }

    public void putSize(int size) {
      this.buffer.putInt(12, size);
    }

    public void complete() {
      this.buffer.put(16, (byte) 0x1);
    }
  }

  public static void main(String[] args) {

    Random r = new Random(System.currentTimeMillis());
    while (true) {
      System.out.println("start");
      int vol = 65025;
      long[] arr = //          new long[] {
          //            -8711702354532787217L,
          //            -4672954889655117952L,
          //            -3321089876154678955L,
          //            -3064927235456487801L,
          //            94222087756875296L,
          //            5541791940509933611L,
          //            5682020396296900800L,
          //            6780209121717038309L,
          //            7378881157325446112L,
          //            9219297080670160801L
          //          };
          IntStream.range(0, vol)
              .mapToLong(l -> r.nextInt(vol))
              // .boxed()
              .sorted()
              .toArray();
      BTree tree = BTree.create(vol, 4096);
      //      new BTree(
      //          vol, 4096, new BTree.Header(ByteBuffer.allocate(1024)), i ->
      // ByteBuffer.allocate(i));
      //      tree.init();
      int i = 0;
      while (i < vol) {
        tree.put(arr[i], arr[i] * 10);
        i++;
      }
      tree.complete();

      i = 0;
      while (i < vol) {
        long ret = tree.find(arr[i]);
        //    System.out.println("key:" + arr[i] + " val:" + (arr[i] * 10));
        if (ret != arr[i] * 10) {
          throw new RuntimeException("error :" + ret);
        }
        i++;
      }
      System.out.println("arr:" + Arrays.toString(arr));
      tree.findGt(0).forEachRemaining(System.out::println);
      System.out.println("done");
    }

    // System.out.println(tree.find(18));
    // tree.findGt(100).forEachRemaining(System.out::println);
  }
}
