//package test;
//
//
//import com.cloudimpl.db4ji2.core.old.LongComparable;
//import com.google.common.collect.Iterators;
//import com.google.common.collect.PeekingIterator;
//import java.util.Comparator;
//import java.util.Iterator;
//import java.util.PriorityQueue;
//import java.util.Queue;
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
//public abstract class MergingIterator<T extends QueryBlock>{
//
//    final Queue<QueryBlock.Iterator> queue;
//    private final QueryBlock.Iterator[] idx;
//    public MergingIterator(final QueryBlockComparator<T> comparator,QueryBlock.Iterator... iterators) {
//        // A comparator that's used by the heap, allowing the heap
//        // to be sorted based on the top of each iterator.
//        Comparator<QueryBlock.Iterator> heapComparator
//                = (QueryBlock.Iterator left, QueryBlock.Iterator right) -> comparator.compare((T)left.getQueryBlock(), left.peekInt(), (T)right.getQueryBlock(), right.peekInt());
//
//        queue = new PriorityQueue<>(2, heapComparator);
//        idx = new QueryBlock.Iterator[iterators.length];
//        init(iterators);
//    }
//
//    public final void init(QueryBlock.Iterator... iterators)
//    {
//        queue.clear();
//        int i = 0;
//        for (QueryBlock.Iterator iterator : iterators) {
//            if (iterator.hasNext()) {
//                queue.add(iterator.withIteratorIndex(i));
//                idx[i] = iterator;
//                i++;
//            }
//        }
//        
//    }
//    public boolean hasNext() {
//        return !queue.isEmpty();
//    }
//
//    public long next() {
//        QueryBlock.Iterator nextIter = queue.remove();
//        int next = nextIter.nextInt();
//        long idx = nextIter.iteratorIdx();
//        if (nextIter.hasNext()) {
//            queue.add(nextIter);
//        }
//        return (idx << 32) | next  & 0xFFFFFFFFFFFFFFFFL;
//    }
//    
//    public T getQueryBlock(int idx)
//    {
//        return (T)this.idx[idx].getQueryBlock();
//    }
//}
