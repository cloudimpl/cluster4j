/*
 * Copyright 2021 nuwan.
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
package test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import org.green.jelly.JsonNumber;
import org.green.jelly.MutableJsonNumber;

/**
 *
 * @author nuwan
 */
public class NumberMergingIterator {
    
    private final Queue<NumberQueryBlock.Iterator> queue;
    private final MutableJsonNumber left;
    private final MutableJsonNumber right;
    private int totalItemCount;
    private long maxKey;
    private long minKey;
    private int maxExp;
    public NumberMergingIterator(int initialCapacity) {
        // A comparator that's used by the heap, allowing the heap
        // to be sorted based on the top of each iterator.
        Comparator<NumberQueryBlock.Iterator> heapComparator
                = (NumberQueryBlock.Iterator l, NumberQueryBlock.Iterator r) -> this.compare(l.getQueryBlock(), l.peekInt(), r.getQueryBlock(), r.peekInt());

        queue = new PriorityQueue<>(initialCapacity, heapComparator);
        this.left = new MutableJsonNumber();
        this.right = new MutableJsonNumber();
        reset();
    }

    public final void reset()
    {
        this.queue.clear();
        this.totalItemCount = 0;
        this.maxKey = Long.MIN_VALUE;
        this.minKey = Long.MAX_VALUE;
        this.maxExp = 0;
    }
    
    public final NumberMergingIterator add(NumberQueryBlock.Iterator iterator)
    {
        if(iterator.hasNext())
        {
            NumberQueryBlock queryBlock = iterator.getQueryBlock();
            this.totalItemCount += queryBlock.getSize();
            this.maxKey = Math.max(this.maxKey, queryBlock.getMaxKeyAsLong());
            this.minKey = Math.min(this.minKey, queryBlock.getMinKeyAsLong());
            this.maxExp = Math.max(this.maxExp, queryBlock.getMaxExp());
            queue.add(iterator);
        }
        return this;
    }
    
    public int getTotalItemCount()
    {
        return this.totalItemCount;
    }
    
    public long getMaxKey()
    {
        return this.maxKey;
    }
    
    public long getMinKey()
    {
        return this.minKey;
    }
    
    public int getMaxExp()
    {
        return this.maxExp;
    }
    
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    public int getSize()
    {
        return queue.size();
    }
    
    public NumberEntry next(NumberEntry entry) {
        NumberQueryBlock.Iterator nextIter = queue.remove();
        int next = nextIter.nextInt();
        if (nextIter.hasNext()) {
            queue.add(nextIter);
        }
        return nextIter.getQueryBlock().getEntry(next, entry);
    }
    
    private int compare(NumberQueryBlock left,int leftIndex, NumberQueryBlock right , int rightIndex)
    {
        JsonNumber leftJson = left.getKey(leftIndex,this.left);
        JsonNumber rightJson = right.getKey(rightIndex,this.right);
        
        int leftExp = Math.abs(leftJson.exp());
        int  rigtExp = Math.abs(rightJson.exp());
        int max = Math.max(leftExp,rigtExp);
        return Long.compare(leftJson.mantissa() * NumberMemBlock.lookupTable[max - leftExp], rightJson.mantissa() * NumberMemBlock.lookupTable[max - rigtExp]);
    }
}
