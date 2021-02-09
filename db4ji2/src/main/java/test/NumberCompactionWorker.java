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

import com.cloudimpl.db4ji2.core.old.CompactionException;

/**
 *
 * @author nuwan
 */
public class NumberCompactionWorker extends CompactionWorker {

    private final NumberQueryBlock.Iterator[] iterators;
    private final NumberMergingIterator iterator;
    private final NumberEntry numberEntry;

    public NumberCompactionWorker(int level, ColumnIndex idx, int batchCount) {
        super(level, idx);
        iterators = new NumberQueryBlock.Iterator[batchCount];
        iterator = new NumberMergingIterator(batchCount);
        this.numberEntry = new NumberEntry();
        initIterators();
    }

    private void initIterators() {
        int i = 0;
        while (i < iterators.length) {
            iterators[i++] = new NumberQueryBlock.Iterator<>();
        }
    }

    public void submit(NumberQueryBlock queryBlock) {
        iterator.add(queryBlock.all(iterators[iterator.getSize()]));
        if (iterator.getSize() == iterators.length) {
            compact();
            iterator.reset();
        }

    }

    @Override
    public NumberQueryBlock compact() {
        if (iterator.getMaxExp() == 0) {
            Class<?> type = getType(iterator.getMinKey(), iterator.getMaxKey(), iterator.getMaxExp());
            NumberQueryBlock queryBlock = createBTree(type, iterator, iterator.getMaxExp(), iterator.getTotalItemCount());
            return queryBlock;
        } else {
            return createBTree(double.class, iterator, iterator.getMaxExp(), iterator.getTotalItemCount());
        }
    }

    private NumberQueryBlock createBTree(Class<?> type, NumberMergingIterator ite, int maxExp, int totalCount) {
        if (type == byte.class) {
            return createByteBTree(ite, totalCount);
        } else if (type == short.class) {
            return createShortBTree(ite, totalCount);
        } else if (type == int.class) {
            return createIntBTree(ite, totalCount);
        } else if (type == long.class) {
            return createLongBTree(ite, totalCount);
        } else if (type == double.class) {
            return createDecimalBTree(ite, maxExp, totalCount);
        } else {
            throw new CompactionException("unknown type:" + type);
        }
    }

    private NumberQueryBlock createByteBTree(NumberMergingIterator ite, int totalCount) {
        ByteBtree btree = getIdx().createBTree(byte.class, 0, totalCount);
        while (ite.hasNext()) {
            ite.next(numberEntry);
            btree.put(numberEntry.getKeyAsByte(), numberEntry.getValue());
        }
        return btree;
    }

    private NumberQueryBlock createShortBTree(NumberMergingIterator ite, int totalCount) {
        ShortBtree btree = getIdx().createBTree(short.class, 0, totalCount);
        while (ite.hasNext()) {
            ite.next(numberEntry);
            btree.put(numberEntry.getKeyAsShort(), numberEntry.getValue());
        }
        return btree;
    }

    private NumberQueryBlock createIntBTree(NumberMergingIterator ite, int totalCount) {
        IntBtree btree = getIdx().createBTree(int.class, 0, totalCount);
        while (ite.hasNext()) {
            ite.next(numberEntry);
            btree.put(numberEntry.getKeyAsInt(), numberEntry.getValue());
        }
        return btree;
    }

    private NumberQueryBlock createLongBTree(NumberMergingIterator ite, int totalCount) {
        Long2Btree btree = getIdx().createBTree(long.class, 0, totalCount);
        while (ite.hasNext()) {
            ite.next(numberEntry);
            btree.put(numberEntry.getKeyAsInt(), numberEntry.getValue());
        }
        return btree;
    }

    private NumberQueryBlock createDecimalBTree(NumberMergingIterator ite, int maxExp, int totalCount) {
        DecimalBtree btree = getIdx().createBTree(long.class, maxExp, totalCount);
        while (ite.hasNext()) {
            ite.next(numberEntry);
            btree.put(numberEntry.getKey(), numberEntry.getValue());
        }
        return btree;
    }

    private Class<?> getType(long minKey, long maxKey, int maxExp) {
        if (maxExp > 0) {
            return double.class;
        } else if (minKey >= Byte.MIN_VALUE && maxKey <= Byte.MAX_VALUE) {
            return byte.class;
        } else if (minKey >= Short.MIN_VALUE && maxKey <= Short.MAX_VALUE) {
            return short.class;
        } else if (minKey >= Integer.MIN_VALUE && maxKey <= Integer.MAX_VALUE) {
            return int.class;
        }
        {
            return long.class;
        }
    }
}
