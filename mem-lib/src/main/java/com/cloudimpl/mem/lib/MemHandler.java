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
package com.cloudimpl.mem.lib;

/**
 *
 * @author nuwan
 */
public interface MemHandler {
    
    void set(byte v);

    void set(short v);

    void set(int v);

    void set(long v);
    
    void set(double v);
    
    void set(long offset,byte v);

    void set(long offset,short v);

    void set(long offset,int v);

    void set(long offset,long v);
    
    void set(long offset,double v);

    void set(long nodeIdx, long itemIdx, byte v);

    void set(long nodeIdx, long itemIdx, short v);

    void set(long nodeIdx, long itemIdx, int v);

    void set(long nodeIdx, long itemIdx, long v);

    void set(long nodeIdx, long itemIdx, double v);

    byte getByte(long nodeIdx, long itemIdx);

    short getShort(long nodeIdx, long itemIdx);

    int getInt(long nodeIdx, long itemIdx);

    long getLong(long nodeIdx, long itemIdx);
   
    double getDouble(long nodeIdx, long itemIdx);
    
    byte getByte();

    short getShort();

    int getInt();

    long getLong();
    
    double getDouble();
    
    byte getByte(long offset);

    short getShort(long offset);

    int getInt(long offset);

    long getLong(long offset);
    
    double getDouble(long offset);

}
