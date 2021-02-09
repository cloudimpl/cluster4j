//package test;
//
//import com.cloudimpl.db4ji2.core.old.Validation;
//import com.google.common.collect.Iterators;
//import jdk.incubator.foreign.MemorySegment;
//
///*
// * Copyright 2021 nuwan.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
///**
// *
// * @author nuwan
// */
//public class LongMergingIterator extends MergingIterator<Long2Btree> {
//
//    public LongMergingIterator(QueryBlockComparator<Long2Btree> comparator, Long2Btree.Iterator... iterators) {
//        super(comparator, iterators);
//    }
//
//    public LongEntry nextEntry(LongEntry entry) {
//        long val = next();
//        int btreeIdx = (int) ((val >> 32) & 0xFFFFFFFFL);
//        int keyIdx = (int) val;
//        Long2Btree btree = getQueryBlock(btreeIdx);
//        return btree.getEntry(keyIdx, entry);
//    }
//
//    public static void main(String[] args) {
//        int vol = 30000000;
//        Long2Btree btree = new Long2Btree(vol, 4096, layout -> MemorySegment.allocateNative(layout), Long::compare);
//        System.out.println("size: " + btree.memSize());
//        System.gc();
//        int j = 0;
//        LongEntry entry = new LongEntry();
//        LongMergingIterator mergeIte = null;
//        NumberQueryBlock.Iterator ite2 = new NumberQueryBlock.Iterator();
//        NumberQueryBlock.Iterator ite3 = new NumberQueryBlock.Iterator();
//        while (j < 100000) {
//            btree.reset();
//            int k = 0;
//            long start = System.currentTimeMillis();
//            while (k < vol) {
//                btree.put(k, k * 10);
//                k++;
//            }
//            btree.complete();
//            long end = System.currentTimeMillis();
//            System.out.println("write:" + (end - start));
//            start = System.currentTimeMillis();
//            k = 0;
//            while (k < vol) {
//                Long2Btree.Iterator ite = btree.findEq(ite2, k);
//                long v = btree.getValue(ite.nextInt());
//                if (v != k * 10) {
//                    System.out.println("invalid val:" + v + " k : " + k);
//                }
//                k++;
//            }
//            end = System.currentTimeMillis();
//            System.out.println("read:" + (end - start) + "ops:" + (((double) (end - start) * 1000)) / vol);
//
//            QueryBlock.Iterator ite = btree.findGE(ite3, 0);
//            ite.forEachRemaining((int i) -> Validation.checkCondition(btree.getKey(i) * 10 == btree.getValue(i), () -> "value not equal"));
//            int b = 0;
//            while (b < 100000) {
//                if (mergeIte == null) {
//                    mergeIte = new LongMergingIterator(new QueryBlockComparator<Long2Btree>() {
//                        @Override
//                        public int compare(Long2Btree leftBtree, int left, Long2Btree rightBTree, int right) {
//                            return Long.compare(((Long2Btree) leftBtree).getKey(left), ((Long2Btree) rightBTree).getKey(right));
//                        }
//                    }, btree.findGE(ite3, 0));
//                } else {
//                    mergeIte.init(btree.findGE(ite3, 0));
//                }
//                //    System.out.println("merge started");
//                start = System.currentTimeMillis();
//                k = 0;
//                while (mergeIte.hasNext()) {
//                    LongEntry e = mergeIte.nextEntry(entry);
//                    if (e.getKey() * 10 != e.getValue()) {
//                        System.out.println("entry: " + entry + " check :" + btree.getKey(512) + ": " + btree.getValue(512));
//                        throw new RuntimeException("xxx");
//                    }
//                    k++;
//                }
//                if (k != vol) {
//                    throw new RuntimeException("error");
//                }
//                end = System.currentTimeMillis();
//                System.out.println("merge:" + (end - start) + "ops:" + (((double) (end - start) * 1000)) / k);
//                b++;
//            }
//
//            j++;
//        }
//    }
//}
