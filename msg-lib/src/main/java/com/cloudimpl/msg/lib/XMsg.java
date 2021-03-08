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
public abstract class  XMsg {
    protected abstract void putByte(int offset,byte b);
    protected abstract void putChar(int offset,char c);
    protected abstract void putBoolean(int offset,boolean b);
    protected abstract void putShort(int offset ,short s);
    protected abstract void putInt(int offset,int i);
    protected abstract void putLong(int offset,long l);
    protected abstract void putCharSequence(int offset,CharSequence val);
    
    protected abstract byte getByte(int offset);
    protected abstract char getChar(int offset);
    protected abstract boolean getBoolean(int offset);
    protected abstract short getShort(int offset);
    protected abstract int getInt(int offset);
    protected abstract long getLong(int offset);
    protected abstract CharSequence getCharSequence(XCharSequence holder,int offset);
}
