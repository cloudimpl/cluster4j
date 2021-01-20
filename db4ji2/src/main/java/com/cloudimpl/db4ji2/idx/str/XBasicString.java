package com.cloudimpl.db4ji2.idx.str;

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
/**
 *
 * @author nuwan
 */
public class XBasicString implements XCharSequence {

    private CharSequence charSeq;

    public void init(CharSequence charSeq) {
        this.charSeq = charSeq;
    }

    @Override
    public char nextChar(long pos) {
        if (pos >= charSeq.length()) {
            return (char) 0x00;
        }
        return charSeq.charAt((int) pos);
    }
    
    public boolean isEmpty()
    {
        return this.charSeq == null;
    }
    
    @Override
    public String toString() {
        return charSeq.toString();
    }

    
}
