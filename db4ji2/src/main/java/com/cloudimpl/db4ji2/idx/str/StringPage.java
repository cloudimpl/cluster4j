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
package com.cloudimpl.db4ji2.idx.str;

import java.nio.ByteBuffer;

/**
 *
 * @author nuwan
 */
public class StringPage {
    private ByteBuffer buf;

    public StringPage(ByteBuffer buf) {
        this.buf = buf;
    }
    
    
    public int getPosition()
    {
        return this.buf.position();
    }
    
    public int getCapacity()
    {
        return this.buf.capacity();
    }
    
    public void append(char c)
    {
        this.buf.put((byte)c);
    }
    
    public char getChar(int index)
    {
        return (char)this.buf.get(index);
    }
    
    public boolean isFull()
    {
        return !this.buf.hasRemaining();
    }
}
