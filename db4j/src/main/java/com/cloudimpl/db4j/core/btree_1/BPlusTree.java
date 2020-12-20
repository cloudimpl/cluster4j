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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import org.xerial.snappy.Snappy;
import reactor.core.publisher.Flux;

/** @author nuwansa */
public class BPlusTree {

  private LongBuffer mainBuf;
  private ByteBuffer byteBuf;
  private int indexNodeCount;
  private int leafNodeCount;
  private int itemsPerNode;
  private WritableByteBufNode currentleafNodeBuf;
  private int currentIndex;
  private ByteBufNode rootLeafNode;
  private int leafOffset;
  private ByteBufNode rootNode;
  private int currentLeafCount;
  private boolean completed = false;
  private int size;
  private int totalItems;

  public BPlusTree(int totalItems, int itemsPerNode) {
    //    if (totalItems % itemsPerNode != 0) {
    //      throw new RuntimeException("invalid argument");
    //    }

    this.itemsPerNode = itemsPerNode;
    this.totalItems = totalItems;
    leafNodeCount = level(totalItems, itemsPerNode);
    indexNodeCount = calculateIndexNodes(leafNodeCount, itemsPerNode);
    System.out.println(
        "leaf:"
            + leafNodeCount
            + " index: "
            + indexNodeCount
            + "cap : "
            + ByteBufNode.getRequiredCapacity(itemsPerNode) * (leafNodeCount + indexNodeCount)
            + "leaf offset : "
            + ByteBufNode.getRequiredCapacity(this.itemsPerNode) * this.indexNodeCount);

    System.out.println("node cap:" + ByteBufNode.getRequiredCapacity(itemsPerNode) * 8);
    this.leafOffset = ByteBufNode.getRequiredCapacity(this.itemsPerNode) * this.indexNodeCount;
    //   byteBuf = ByteBuffer.allocate((itemsPerNode) * (leafNodeCount + indexNodeCount) * 8);
    mainBuf =
        LongBuffer.allocate(
            ByteBufNode.getRequiredCapacity(itemsPerNode) * (leafNodeCount + indexNodeCount));
    currentleafNodeBuf =
        new WritableByteBufNode(
            mainBuf
                .position(ByteBufNode.getRequiredCapacity(this.itemsPerNode) * this.indexNodeCount)
                .slice()
                .limit(ByteBufNode.getRequiredCapacity(itemsPerNode)));
    this.rootLeafNode = currentleafNodeBuf;
    this.currentLeafCount = 1;
    this.size = 0;
  }

  public int capacity() {
    return mainBuf.capacity() * 8;
  }

  public boolean put(long key, long value) {
    if (this.size == this.totalItems) {
      return false;
    }
    if (currentIndex == itemsPerNode) {
      if (currentLeafCount == leafNodeCount) {
        return false;
      }
      WritableByteBufNode newBuf = nextWritableNode(currentleafNodeBuf);
      currentleafNodeBuf.setSize(itemsPerNode);
      // System.out.println("leaf : " + currentleafNodeBuf);
      currentleafNodeBuf = newBuf;
      currentIndex = 0;
      currentLeafCount++;
    }
    currentleafNodeBuf.setKey(currentIndex, key);
    currentleafNodeBuf.setLeft(currentIndex, value);
    currentIndex++;
    this.size++;
    return true;
  }

  public long find(long key) {
    ByteBufNode current = rootNode;
    while (current.getId() < leafOffset) {
      long pos = current.findChild(key);
      //  System.out.println("pos :" + pos);
      current =
          new ByteBufNode(
              mainBuf
                  .position((int) pos)
                  .slice()
                  .limit(ByteBufNode.getRequiredCapacity(this.itemsPerNode)));
    }
    // System.out.println("leaf: " + current);
    return current.find(key);
  }

  public Flux<Long> findGt(long key, int limit) {
    ByteBufNode current = rootNode;
    while (current.getId() < leafOffset) {
      long pos = current.findChild(key);
      //  System.out.println("pos :" + pos);
      current =
          new ByteBufNode(
              mainBuf
                  .position((int) pos)
                  .slice()
                  .limit(ByteBufNode.getRequiredCapacity(this.itemsPerNode)));
    }
    int index = current.findGe(key);
    ByteBufNode leaf = current;
    return Flux.fromIterable(() -> iterator(leaf, index)).take(limit);
    //     .doOnNext(l -> System.out.println(this + ":" + l));
  }

  private WritableByteBufNode nextWritableNode(WritableByteBufNode currentNode) {
    WritableByteBufNode nextNode =
        new WritableByteBufNode(
            mainBuf
                .position(
                    currentNode.buffer.arrayOffset()
                        + ByteBufNode.getRequiredCapacity(this.itemsPerNode))
                .slice()
                .limit(ByteBufNode.getRequiredCapacity(itemsPerNode)));
    currentNode.setNext(nextNode);
    return nextNode;
  }

  private ByteBufNode nextNode(ByteBufNode currentNode) {
    int pos = currentNode.getNext();
    if (pos == 0) {
      return null;
    }
    return new ByteBufNode(
        mainBuf.position(pos).slice().limit(ByteBufNode.getRequiredCapacity(itemsPerNode)));
  }

  private void fillLevel(ByteBufNode levelNode, int nodeCount) {

    System.out.println(
        "level offset: "
            + (levelNode.buffer.arrayOffset()
                - (level(nodeCount, itemsPerNode) * ByteBufNode.getRequiredCapacity(itemsPerNode)))
            + "nodeccounts:"
            + nodeCount);
    WritableByteBufNode firstNode =
        new WritableByteBufNode(
            mainBuf
                .position(
                    levelNode.buffer.arrayOffset()
                        - (level(nodeCount, itemsPerNode)
                            * ByteBufNode.getRequiredCapacity(itemsPerNode)))
                .slice()
                .limit(ByteBufNode.getRequiredCapacity(itemsPerNode)));

    int levelNodeCount = 1;
    ByteBufNode currentBelowNode = levelNode;
    WritableByteBufNode currentNode = firstNode;
    ByteBufNode nextNode = null;
    while (true) {
      int i = 0;
      while (i < itemsPerNode) {
        nextNode = nextNode(currentBelowNode);
        if (nextNode == null) {
          currentNode.setRight(i - 1, currentBelowNode.getId());
          break;
        }
        currentNode.setKey(i, nextNode.getKey(0));
        currentNode.setLeft(i, currentBelowNode.getId());
        currentBelowNode = nextNode;
        i++;
      }
      currentNode.setSize(i);
      currentNode.setRight(i - 1, currentBelowNode.getId());
      //  System.out.println("Index : node:" + currentNode);
      if (nextNode == null || nextNode(nextNode) == null) {
        break;
      }
      currentNode = nextWritableNode(currentNode);
      levelNodeCount++;
    }
    if (levelNodeCount > 1) {
      fillLevel(firstNode, levelNodeCount);
    } else {
      rootNode = firstNode;
    }
  }

  public void complete() {
    if (completed) {
      return;
    }
    ByteBufNode.Writer.updateSize(currentleafNodeBuf, currentIndex);
    //    System.out.println("leaf : " + currentleafNodeBuf);
    fillLevel(rootLeafNode, leafNodeCount);
    try {
      byte[] b = Snappy.compress(mainBuf.array());
      System.out.println("compress : " + b.length);
    } catch (IOException ex) {
      Logger.getLogger(BPlusTree.class.getName()).log(Level.SEVERE, null, ex);
    }
    completed = true;
  }

  private int level(int perLevelCount, int itemsPerNode) {
    return (int) Math.ceil((double) perLevelCount / itemsPerNode);
  }

  public NodeIterator nodeIterator(ByteBufNode firstNode) {
    return new NodeIterator(mainBuf, firstNode == null ? rootLeafNode : firstNode);
  }

  public Iterator iterator(ByteBufNode firstNode, int index) {
    return new Iterator(nodeIterator(firstNode), index);
  }

  private int calculateIndexNodes(int leafCount, int itemsPerNode) {
    int count = level(leafCount, itemsPerNode);
    if (count <= 1) {
      return 1;
    }
    return count + calculateIndexNodes(count, itemsPerNode);
  }

  public static final class NodeIterator {

    private final LongBuffer mainBuffer;
    private final ByteBufNode firstNode;
    private ByteBufNode current;

    public NodeIterator(LongBuffer mainBuffer, ByteBufNode firstNode) {
      this.mainBuffer = mainBuffer;
      this.firstNode = firstNode;
      this.current = this.firstNode;
    }

    public boolean hasNext() {
      return current != null;
    }

    public ByteBufNode getNode() {
      return this.current;
    }

    public void moveToNext() {
      if (current != null) {
        int pos = current.getNext();
        if (pos == 0) {
          this.current = null;
          return;
        }
        this.current =
            new ByteBufNode(mainBuffer.position(pos).slice().limit(this.current.buffer.limit()));
      }
    }
  }

  public static final class Iterator implements java.util.Iterator<Long> {

    private final NodeIterator nodeIterator;
    private ByteBufNode current;
    private ByteBufNode.Iterator ite;

    public Iterator(NodeIterator nodeIterator, int index) {
      this.nodeIterator = nodeIterator;
      if (this.nodeIterator.hasNext()) {
        this.ite = this.nodeIterator.getNode().iterator(index);
      } else {
        this.ite = new ByteBufNode.Iterator(null);
      }
    }

    @Override
    public boolean hasNext() {
      return this.ite.hasNext();
    }

    @Override
    public Long next() {
      long k = this.ite.getKey();
      moveToNext();
      return k;
    }

    public void moveToNext() {
      this.ite.moveToNext();
      if (!this.ite.hasNext() && this.nodeIterator.hasNext()) {
        this.nodeIterator.moveToNext();
        if (this.nodeIterator.hasNext()) this.ite = this.nodeIterator.getNode().iterator();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {

    int vol = 1_000_000;
    int itemCount = 253;
    BPlusTree tree = new BPlusTree(vol, itemCount);
    int i = 0;

    System.out.println("node cap:" + ByteBufNode.getRequiredCapacity(itemCount) * 8);
    System.out.println("space :" + (vol * 16) / 1024 + " tree cap :" + tree.capacity() / 1024);
    long start = System.currentTimeMillis();
    while (i < vol) {
      tree.put(i, i * 10);
      i++;
    }
    tree.complete();
    long end = System.currentTimeMillis();
    System.out.println("ops:" + ((end - start == 0) ? "inifity" : (vol / (end - start)) * 1000));

    List<Integer> list =
        Arrays.<Integer>asList(IntStream.range(0, vol).boxed().toArray(Integer[]::new));
    Collections.shuffle(list);
    System.gc();
    i = 0;
    start = System.currentTimeMillis();
    while (i < list.size()) {
      long key = list.get(i);
      long val = tree.find(key);
      if (val != key * 10) {
        throw new RuntimeException("invalid value :" + key + ":" + val);
      }
      i++;
    }
    end = System.currentTimeMillis();
    System.out.println("ops:" + ((end - start == 0) ? "inifity" : (vol / (end - start)) * 1000));

    tree.findGt(10000, 10)
        .doOnNext(System.out::println)
        .doOnError(thr -> thr.printStackTrace())
        .subscribe();
    Thread.sleep(100000000);
    // BPlusTree.NodeIterator ite2 = tree.nodeIterator(null);
    //    while (ite2.hasNext()) {
    //      ByteBufNode.Iterator ite3 = ite2.getNode().iterator();
    //      while (ite3.hasNext()) {
    //        //   System.out.println("key:" + ite3.getKey() + " value : " + ite3.getLeft());
    //        ite3.moveToNext();
    //      }
    //      ite2.moveToNext();
    //    }
  }
}
