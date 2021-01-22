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
package com.cloudimpl.db4ji2.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/** @author nuwansa */
public class LongBTree extends ReadOnlyLongBTree {

  private WritableLeafNode rootLeafNode;
  private WritableLeafNode currentLeafNode;
  private int currentIndex;
  private int size;
  private LongBTree.Header header;
  private LongComparable comparator;
  public LongBTree(
      int maxItemCount,
      int pageSize,
      LongBTree.Header header,
      Function<Integer, ByteBuffer> bufferProvider,LongComparable comparator,Supplier<? extends Entry> entrySupplier) {
    super(header, bufferProvider,comparator,entrySupplier);
    this.header = header;
    this.maxItemCount = maxItemCount;
    this.pageSize = pageSize;
  }

  @Deprecated
  public static LongBTree create(String fileName, int maxItemCount, int pageSize,LongComparable comparator,Supplier<? extends Entry> entrySupplier) {
    try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
      LongBTree tree =
          new LongBTree(
              maxItemCount,
              pageSize,
              new LongBTree.Header(getByteBuf(file, FileChannel.MapMode.READ_WRITE, 0, 1024)),
              size -> getByteBuf(file, FileChannel.MapMode.READ_WRITE, 1024, size),comparator,entrySupplier);
      tree.init();
      return tree;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Deprecated
  public static LongBTree create(int maxItemCount, int pageSize,LongComparable comparator,Supplier<? extends Entry> entrySupplier) {
    LongBTree tree =
        new LongBTree(
            maxItemCount,
            pageSize,
            new LongBTree.Header(ByteBuffer.allocateDirect(1024)),
            size -> ByteBuffer.allocateDirect(size),comparator,entrySupplier);
    tree.init();
    return tree;
  }

  public static LongBTree create(int maxItemCount,int pageSize,Function<Integer,ByteBuffer> btreeBufferProvider,Function<Integer,ByteBuffer> headerBufferProvider,LongComparable comparator,Supplier<? extends Entry> entrySupplier)
  {
      LongBTree tree =
        new LongBTree(
            maxItemCount,
            pageSize,
            new LongBTree.Header(headerBufferProvider.apply(1024)),
            size -> btreeBufferProvider.apply(size),comparator,entrySupplier);
    tree.init();
    return tree;
  }
  
  public static ReadOnlyLongBTree from(String fileName,LongComparable comparator,Supplier<? extends Entry> entrySupplier) {
    try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
      ReadOnlyLongBTree tree =
          new ReadOnlyLongBTree(
              new LongBTree.Header(getByteBuf(file, FileChannel.MapMode.READ_ONLY, 0, 1024)),
              size -> getByteBuf(file, FileChannel.MapMode.READ_ONLY, 1024, size),comparator,entrySupplier);
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
  public void init() {
    this.maxItemPerNode = getMaxItemPerNode(pageSize);
    this.leafNodeCounts = getLeafNodeCount(maxItemCount, maxItemPerNode);
    this.indexNodeCount = getTotalIndexNodeCount(this.leafNodeCounts, maxItemPerNode);
    this.mainBuffer = bufferProvider.apply(pageSize * (this.leafNodeCounts + this.indexNodeCount));
    this.rootLeafNode =
        new WritableLeafNode(mainBuffer, pageSize * indexNodeCount, maxItemPerNode, pageSize);
    this.currentLeafNode = this.rootLeafNode;
    this.currentIndex = 0;
  }

  public int getBufSize()
  {
    return this.mainBuffer.capacity();
  }
  
  protected void reset(ByteBuffer blankPage) {
    this.currentIndex = 0;
    blankPage.position(0).limit(blankPage.capacity());
    this.size = 0;
    mainBuffer.clear();
    while (mainBuffer.remaining() > 0) {
      mainBuffer.put(blankPage);
      blankPage.position(0).limit(blankPage.capacity());
    }
    this.rootLeafNode =
        new WritableLeafNode(mainBuffer, pageSize * indexNodeCount, maxItemPerNode, pageSize);
    this.currentLeafNode = this.rootLeafNode;
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

  public void close() {
    closeDirectBuffer(mainBuffer);
    header.close();
  }

  public static void closeDirectBuffer(ByteBuffer cb) {
    if (cb == null || !cb.isDirect()) {
      return;
    }
    // we could use this type cast and call functions without reflection code,
    // but static import from sun.* package is risky for non-SUN virtual machine.
    // try { ((sun.nio.ch.DirectBuffer)cb).cleaner().clean(); } catch (Exception ex) { }

    // JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
    boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
    try {
      if (isOldJDK) {
        Method cleaner = cb.getClass().getMethod("cleaner");
        cleaner.setAccessible(true);
        Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
        clean.setAccessible(true);
        clean.invoke(cleaner.invoke(cb));
      } else {
        Class unsafeClass;
        try {
          unsafeClass = Class.forName("sun.misc.Unsafe");
        } catch (Exception ex) {
          // jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
          // but that method should be added if sun.misc.Unsafe is removed.
          unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
        }
        Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        clean.setAccessible(true);
        Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
        theUnsafeField.setAccessible(true);
        Object theUnsafe = theUnsafeField.get(null);
        clean.invoke(theUnsafe, cb);
      }
    } catch (ClassNotFoundException
        | IllegalAccessException
        | IllegalArgumentException
        | NoSuchFieldException
        | NoSuchMethodException
        | SecurityException
        | InvocationTargetException ex) {
    }
    cb = null;
  }

  public void complete() {
    // System.out.println("complete:" + maxItemCount + ":" + leafNodeCounts);
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

  public static final class Header extends ReadOnlyLongBTree.Header {

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

    public void close() {
      closeDirectBuffer(buffer);
    }

    public int getCapacity() {
      return buffer.capacity();
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
              .mapToLong(l -> l)
              // .boxed()
              .sorted()
              .toArray();
      LongBTree tree = LongBTree.create(vol, 4096,Long::compare,()->new LongEntry());
      //      new BTree(
      //          vol, 4096, new BTree.Header(ByteBuffer.allocate(1024)), i ->
      // ByteBuffer.allocate(i));
      //      tree.init();
      int i = 0;
      while (i < vol) {
        tree.put(arr[i],  (arr[i] * 10) + i);
        i++;
      }
      tree.complete();

      ByteBuffer blank = ByteBuffer.allocate(4096);
      tree.reset(blank);
      i = 0;
      while (i < vol) {
        tree.put(arr[i], (arr[i] * 10) + i);
        i++;
      }
      tree.complete();

      i = 0;
      while (i < vol) {
        long ret = tree.find(arr[i]);
        //    System.out.println("key:" + arr[i] + " val:" + (arr[i] * 10));
        if (ret != ((arr[i] * 10) + i)) {
         // throw new RuntimeException("error :" + ret);
            System.out.println("error :"+ret + " expect : "+((arr[i] * 10) + 1));
        }
        i++;
      }
      System.out.println("arr:" + Arrays.toString(arr));
      tree.findGT(65010).forEachRemaining(System.out::println);
      System.out.println("done");
      break;
    }

    // System.out.println(tree.find(18));
    // tree.findGt(100).forEachRemaining(System.out::println);
  }
}
