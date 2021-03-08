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

/**
 *
 * @author nuwan
 */
public class NativeXMsg extends XMsg{
    private MemHandler memHandler;
    @Override
    protected void putByte(int offset, byte b) {
        memHandler.set(offset,b);
    }

    @Override
    protected void putChar(int offset, char c) {
        memHandler.set(offset,(byte)c);
    }

    @Override
    protected void putBoolean(int offset, boolean b) {
        memHandler.set(offset,b ? 1: 0);
    }

    @Override
    protected void putShort(int offset, short s) {
        memHandler.set(offset,s);
    }

    @Override
    protected void putInt(int offset, int i) {
        memHandler.set(offset,i);
    }

    @Override
    protected void putLong(int offset, long l) {
        memHandler.set(offset,l);
    }

    @Override
    protected void putCharSequence(int offset, CharSequence val) {
        int len = val.length();
       while(len > 0)
       {
           memHandler.set(offset++, (char)val.charAt(val.length() - len));
           len--;
       }
    }

    @Override
    protected byte getByte(int offset) {
        return memHandler.getByte(offset);
    }

    @Override
    protected char getChar(int offset) {
        return (char)memHandler.getByte(offset);
    }

    @Override
    protected boolean getBoolean(int offset) {
        return memHandler.getByte(offset) == 1;
    }

    @Override
    protected short getShort(int offset) {
        return memHandler.getShort(offset);
    }

    @Override
    protected int getInt(int offset) {
        return memHandler.getInt(offset);
    }

    @Override
    protected long getLong(int offset) {
        return memHandler.getLong(offset);
    }

    @Override
    protected CharSequence getCharSequence(XCharSequence holder, int offset) {
        char c = (char) memHandler.getByte(offset);
        while( c != XCharSequence.NULL)
        {
            holder.put(offset++, c);
            c = (char) memHandler.getByte(offset);
        }
        return holder;
    }
    
}
