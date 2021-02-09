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
public class NumberEntry {
    protected final MutableJsonNumber key;
    protected long value;

    public NumberEntry() {
        key = new MutableJsonNumber();
    }
  
    public NumberEntry with(long mantissa,int exp, long value) {
        this.key.set(mantissa, exp);
        this.value = value;
        return this;
    }

    public long getKeyAsLong() {
        return key.mantissa();
    }

    public int getKeyAsInt() {
        return (int) key.mantissa();
    }

    public short getKeyAsShort() {
        return (short) key.mantissa();
    }

    public byte getKeyAsByte() {
        return (byte) key.mantissa();
    }
    
    public long getValue()
    {
        return this.value;
    }
    
    public JsonNumber getKey()
    {
        return this.key;
    }
    
    @Override
    public String toString() {
        return "NumberEntry{" + "key=" + key + ", value=" + value + '}';
    }
    
    
}
