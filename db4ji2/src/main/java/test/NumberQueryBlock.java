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

    public static final NumberQueryBlock NULL = new NumberQueryBlock() {
        @Override
        public int getMaxExp() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long getMaxKeyAsLong() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long getMinKeyAsLong() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Iterator all(Iterator ite) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public int getSize() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long getKeyAsLong(int index) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long getValue(int index) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };
            
    public static final long[] lookupTable = {1L, 10L, 100L, 1000L, 10000L, 100000L,
        1000_000L, 1000_000_0L, 1000_000_00L, 1000_000_000L,
        1000_000_000_0L, 1000_000_000_00L, 1000_000_000_000L,
        1000_000_000_000_0L, 1000_000_000_000_00L, 1000_000_000_000_000L,
        1000_000_000_000_000_0L, 1000_000_000_000_000_00L
    };
    
    public static final double[] lookupTable2 = {1L, 0.01D, 0.001D, 0.0001D, 0.000_01D, 0.000_001D,
        0.000_000_1D, 0.000_000_01D, 0.000_000_001D, 0.000_000_000_1D,
        0.000_000_000_01D, 0.000_000_000_001D, 0.000_000_000_000_1D,
        0.000_000_000_000_01D,  0.000_000_000_000_001D,  0.000_000_000_000_000_1D,
        0.000_000_000_000_000_01D, 0.000_000_000_000_000_001D
    };
    
    int getMaxExp();

    default double getKeyAsDouble(int index)
    {
        return (double)getKeyAsLong(index);
    }
    
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
        return entry.with(getKeyAsLong(index),0, getValue(index));
    }
    
    default JsonNumber getKey(int index,MutableJsonNumber json)
    {
         json.set(getKeyAsLong(index), 0);
         return json;
    }
    
    default int compare(int index,long rightKeyMantissa,int rightExp)
    {
        return compare(getKeyAsLong(index), 0, rightKeyMantissa, rightExp);
    }
    
    default int compare(long leftKeyMantissa,int leftExp,long rightKeyMantissa,int rightExp)
    {
        return Long.compare(leftKeyMantissa, rightKeyMantissa);// works only when exp = 0
    }
    
    
    public static final class Iterator<T extends NumberQueryBlock> extends QueryBlock.Iterator<T> {

        public static final NumberQueryBlock.Iterator EMPTY = new NumberQueryBlock.Iterator<>();

        private long eqKeyMantissa;
        private int eqKeyExp;
        private boolean checkEq;

        protected Iterator withEqKey(long mantissa, int exp) {
            this.eqKeyMantissa = mantissa;
            this.eqKeyExp = exp;
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
                return getQueryBlock().compare(index, eqKeyMantissa, eqKeyExp) == 0;
            }
            return true;
        }
    }
}
