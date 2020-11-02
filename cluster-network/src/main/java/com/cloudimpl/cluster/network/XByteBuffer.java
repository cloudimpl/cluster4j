/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

/**
 *
 * @author nuwansa
 */
public interface XByteBuffer {

    long addr();
    
    long position();
    
    void position(long offset);
    
    long capacity();

    boolean getBoolean(long offset);

    byte getByte(long offset);

    short getShort(long offset);

    int getInt(long offset);

    long getLong(long offset);

    long getLongVolatile(long offset);
    
    int getIntVolatile(long offset);
    
    long getAndSetLong(long offset,long val);
    
    int getAndSetInt(long offset,int val);
    
    boolean compareAndSwapLong(long offset,long expect,long val);
    
    void putOrderedLong(long offset,long value);
    
    boolean compareAndSwapInt(long pos, int expected, int value);
    
    long getAndAddLong(long offset,long val);
    
    float getFloat(long offset);

    double getDouble(long offset);

    char getChar(long offset);

    boolean getBoolean();

    byte getByte();

    short getShort();

    int getInt();

    long getLong();

    float getFloat();

    double getDouble();

    char getChar();
    
    void putBoolean(boolean v);

    void putByte(byte v);

    void putShort(short v);

    void putInt(int v);

    void putLong(long v);

    void putFloat(float v);

    void putDouble(double v);

    void putChar(char v);
    
    
    void putBoolean(long offset, boolean v);

    void putByte(long offset, byte v);

    void putShort(long offset, short v);

    void putInt(long offset, int v);

    void putLong(long offset, long v);

    void putFloat(long offset, float v);

    void putDouble(long offset, double v);

    void putChar(long offset, char v);
    
    void put(byte[] buf,int offset,int len);
    
    byte[] array();
    
    void close();
    
    XByteBuffer slice(long position);
}
