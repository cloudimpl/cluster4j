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
package com.cloudimpl.msg.lib;

/**
 *
 * @author nuwan
 */
public class ZCharSequence implements CharSequence{
    private final StringBuilder builder;

    public ZCharSequence(int size) {
        this.builder = new StringBuilder(size);
    }
    
    public ZCharSequence() {
        this.builder = new StringBuilder();
    }
    
    public ZCharSequence(CharSequence value) {
        this.builder = new StringBuilder(value.length());
        this.builder.append(value);
    }
    
    public void set(CharSequence value)
    {
        this.builder.setLength(0);
        this.builder.append(value);
    }
    
    @Override
    public final int length() {
        return builder.length();
    }

    @Override
    public final char charAt(int index) {
        return builder.charAt(index);
    }

    @Override
    public final CharSequence subSequence(int start, int end) {
       return builder.subSequence(start, end);
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (int i = 0; i < length(); ++i)
            h = 31 * h + charAt(i);
        return h;
    }

    public void reset()
    {
        this.builder.setLength(0);
    }
    
    public void append(CharSequence value)
    {
        this.builder.append(value);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass() && !CharSequence.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final ZCharSequence other = (ZCharSequence) obj;
        return CharSequence.compare(this.builder, other.builder) == 0;
    }

    @Override
    public String toString() {
        return builder.toString();
    }
    
    
}
