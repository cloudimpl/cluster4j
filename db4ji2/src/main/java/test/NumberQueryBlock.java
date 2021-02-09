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

import org.green.jelly.JsonNumber;
import org.green.jelly.MutableJsonNumber;

/**
 *
 * @author nuwan
 */
public interface NumberQueryBlock extends QueryBlock {

    public static final long[] lookupTable = {1L, 10L, 100L, 1000L, 10000L, 100000L,
        1000_000L, 1000_000_0L, 1000_000_00L, 1000_000_000L,
        1000_000_000_0L, 1000_000_000_00L, 1000_000_000_000L,
        1000_000_000_000_0L, 1000_000_000_000_00L, 1000_000_000_000_000L,
        1000_000_000_000_000_0L, 1000_000_000_000_000_00L
    };
    
    int getMaxExp();

    long getMaxKeyAsLong();

    long getMinKeyAsLong();

    Iterator all(Iterator ite);
    
    default int getKeyAsInt(int index) {
        return (int) getKeyAsLong(index);
    }

    default short getKeyAsShort(int index) {
        return (short) getKeyAsLong(index);
    }

    default byte getKeyAsByte(int index) {
        return (byte) getKeyAsLong(index);
    }

    default NumberEntry getEntry(int index,NumberEntry entry)
    {
        return entry.with(getKeyAsLong(index), -getMaxExp(), getValue(index));
    }
    
    default JsonNumber getKey(int index,MutableJsonNumber json)
    {
         json.set(getKeyAsLong(index), 0);
         return json;
    }
    public static final class Iterator<T extends NumberQueryBlock> extends QueryBlock.Iterator<T> {

        public static final NumberQueryBlock.Iterator EMPTY = new NumberQueryBlock.Iterator<>();

        private long eqKey;
        private boolean checkEq;

        protected Iterator withEqKey(long key) {
            this.eqKey = key;
            this.checkEq = true;
            return this;
        }

        protected Iterator init(T queryBlock, int index, int limit) {
            return super.init(queryBlock, index, limit);
        }

        @Override
        protected T getQueryBlock() {
            return queryBlock;
        }

        @Override
        public boolean hasNext() {
            return super.hasNext() && checkCondition(index);
        }

        private boolean checkCondition(int index) {
            if (checkEq) {
                return getQueryBlock().getKeyAsLong(index) == eqKey;
            }
            return true;
        }
    }
}
