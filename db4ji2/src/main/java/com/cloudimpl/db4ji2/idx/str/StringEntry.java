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

import com.cloudimpl.db4ji2.idx.str.XCharSequence;
import com.cloudimpl.db4ji2.idx.str.StringBlock;
import com.cloudimpl.db4ji2.core.LongEntry;

/**
 *
 * @author nuwan
 */
public class StringEntry extends LongEntry{
    private  StringBlock block;

    public StringEntry() {
    }

    public StringEntry setBlock(StringBlock block) {
        this.block = block;
        return this;
    }
   
    public StringBlock getStringBlock()
    {
        return block;
    }
    
    public static int compare(StringEntry leftEntry, StringEntry rightEntry) {
        long l = leftEntry._getKey();
        long r = rightEntry._getKey();
        
        XCharSequence left = leftEntry.getStringBlock();
        XCharSequence right = rightEntry.getStringBlock();
        
        return XCharSequence.compare(l, left, r, right);
    }
}
