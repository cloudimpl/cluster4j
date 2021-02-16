
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

import java.util.PrimitiveIterator;

/**
 *
 * @author nuwan
 */
public interface QueryBlock {
    //long getStartValue();
    int getSize();
    long getKeyAsLong(int index);
    long getValue(int index);
     long memSize();
     void close();
    public static class Iterator<T extends QueryBlock> implements PrimitiveIterator.OfInt {

        public static final Iterator EMPTY = new Iterator<>();
        protected T queryBlock;
        protected int index;
        protected int limit;
        public Iterator() {
        }

        protected <U extends Iterator<T>> U init(T queryBlock, int index, int limit)
        {
             this.queryBlock = queryBlock;
            this.index = index;
            this.limit = limit;
            return (U)this;
        }
        
        @Override
        public int nextInt() {
            return index++;
        }

        @Override
        public boolean hasNext() {
            return this.queryBlock != null && index < limit;
        }

        public int peekInt() {
            return index;
        }

        protected T getQueryBlock() {
            return queryBlock;
        }

    }
}
