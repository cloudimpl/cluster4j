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

import com.cloudimpl.mem.lib.MemHandler;
import com.cloudimpl.mem.lib.OffHeapMemory;

/**
 *
 * @author nuwan
 */
public class XLogSegment {
    
    private final OffHeapMemory memory;
    private final MemHandler memHandler;
    private int pos;
    private final int size;
    public XLogSegment(OffHeapMemory memory,int size) {
        this.memory = memory;
        this.memHandler = memory.memHandler();
        this.pos = 0;
        this.size = size;
    }
    
    protected int append(final CharSequence record,final int offset,final int len)
    {
        int temp = pos;
        int i= 0;
        
        while(i < (len - offset))
        {
            this.memHandler.set(pos++, (byte)record.charAt(offset + i++));
        }
        return temp;
    }
    
    protected void append(byte val)
    {
        this.memHandler.set(pos++,val);
    }
    
    protected boolean isFull()
    {
        return pos >= size;
    }
    
    public void close()
    {
         memory.close();
    }
    
    public int remaining()
    {
        return this.size - this.pos;
    }
}
