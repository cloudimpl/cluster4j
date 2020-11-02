/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.questdb.std.Unsafe;

/**
 *
 * @author nuwansa
 */
public class ByteBufferNative implements XByteBuffer {

    private final long addr;
    private final long capacity;
    private long position;

    public static final long BYTE_ARRAY_OFFSET;
    
    static{
        BYTE_ARRAY_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);
    }
    
    public ByteBufferNative(long addr, long capacity) {
        this.addr = addr;
        this.capacity = capacity;
        this.position = 0;
    }

    public static XByteBuffer allocate(long size) {
        return new ByteBufferNative(Unsafe.calloc(size), size);
    }

    @Override
    public long position() {
        return this.position;
    }

    @Override
    public void position(long offset) {
        this.position = offset;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public boolean getBoolean(long offset) {
        return Unsafe.getUnsafe().getBoolean(null, addr + offset);
    }

    @Override
    public short getShort(long offset) {
        return Unsafe.getUnsafe().getShort(offset + addr);
    }

    @Override
    public float getFloat(long offset) {
        return Unsafe.getUnsafe().getFloat(offset + addr);
    }

    @Override
    public double getDouble(long offset) {
        return Unsafe.getUnsafe().getDouble(offset + addr);
    }

    @Override
    public char getChar(long offset) {
        return Unsafe.getUnsafe().getChar(offset + addr);
    }

    @Override
    public void putBoolean(boolean v) {
        Unsafe.getUnsafe().putByte(addr + position, v ? (byte) 1 : (byte) 0);
        position += Byte.BYTES;
    }

    @Override
    public void putByte(byte v) {
        Unsafe.getUnsafe().putByte(addr + position, v);
        position += Byte.BYTES;
    }

    @Override
    public void putShort(short v) {
        Unsafe.getUnsafe().putShort(addr + position, v);
        position += Short.BYTES;
    }

    @Override
    public void putInt(int v) {
        Unsafe.getUnsafe().putInt(addr + position, v);
        position += Integer.BYTES;
    }

    @Override
    public void putLong(long v) {
        Unsafe.getUnsafe().putLong(addr + position, v);
        position += Long.BYTES;
    }

    @Override
    public void putFloat(float v) {
        Unsafe.getUnsafe().putFloat(addr + position, v);
        position += Float.BYTES;
    }

    @Override
    public void putDouble(double v) {
        Unsafe.getUnsafe().putDouble(addr + position, v);
        position += Double.BYTES;
    }

    @Override
    public void putChar(char v) {
        Unsafe.getUnsafe().putChar(addr + position, v);
        position += Byte.BYTES;
    }

    @Override
    public void putBoolean(long offset, boolean v) {
        Unsafe.getUnsafe().putByte(offset + addr, v ? (byte) 1 : (byte) 0);
    }

    @Override
    public void putShort(long offset, short v) {
        Unsafe.getUnsafe().putShort(offset + addr, v);
    }

    @Override
    public void putFloat(long offset, float v) {
        Unsafe.getUnsafe().putFloat(offset + addr, v);
    }

    @Override
    public void putDouble(long offset, double v) {
        Unsafe.getUnsafe().putDouble(offset + addr, v);
    }

    @Override
    public void putChar(long offset, char v) {
        Unsafe.getUnsafe().putByte(offset + addr, (byte) v);
    }

    @Override
    public void put(byte[] buf, int offset, int len) {
          Unsafe.getUnsafe().copyMemory(buf, BYTE_ARRAY_OFFSET + offset, null, position + addr, len);
          position += len;
      //  throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean getBoolean() {
        boolean b = Unsafe.getUnsafe().getBoolean(null, position);
        position += Byte.BYTES;
        return b;
    }

    @Override
    public byte getByte() {
        byte b = Unsafe.getUnsafe().getByte(position);
        position += Byte.BYTES;
        return b;
    }

    @Override
    public short getShort() {
        short s = Unsafe.getUnsafe().getShort(position + addr);
        position += Short.BYTES;
        return s;
    }

    @Override
    public int getInt() {
        int i = Unsafe.getUnsafe().getInt(position + addr);
        position += Integer.BYTES;
        return i;
    }

    @Override
    public long getLong() {
        long l = Unsafe.getUnsafe().getLong(position + addr);
        position += Long.BYTES;
        return l;
    }

    @Override
    public float getFloat() {
        float f = Unsafe.getUnsafe().getFloat(position + addr);
        position += Float.BYTES;
        return f;
    }

    @Override
    public double getDouble() {
        double d = Unsafe.getUnsafe().getDouble(position + addr);
        position += Double.BYTES;
        return d;
    }

    @Override
    public char getChar() {
        char c = Unsafe.getUnsafe().getChar(position + addr);
        position += Character.BYTES;
        return c;
    }

    @Override
    public byte getByte(long offset) {
        return Unsafe.getUnsafe().getByte(addr + offset);
    }

    @Override
    public int getInt(long offset) {
        return Unsafe.getUnsafe().getInt(addr + offset);
    }

    @Override
    public long getLong(long offset) {
        return Unsafe.getUnsafe().getLong(addr + offset);
    }

    @Override
    public long getLongVolatile(long offset) {
        return Unsafe.getUnsafe().getLongVolatile(null, addr + offset);
    }

    @Override
    public int getIntVolatile(long offset) {
        return Unsafe.getUnsafe().getIntVolatile(null, addr + offset);
    }

    @Override
    public long getAndSetLong(long offset, long val) {
        return Unsafe.getUnsafe().getAndSetLong(null, offset, val);
    }

    @Override
    public int getAndSetInt(long offset, int val) {
        return Unsafe.getUnsafe().getAndSetInt(null, offset, val);
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expect, long val) {
        return Unsafe.getUnsafe().compareAndSwapLong(null, addr + offset, expect, val);
    }

    @Override
    public void putOrderedLong(long offset, long val) {
        Unsafe.getUnsafe().putOrderedLong(null, addr + offset, val);
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expect, int val) {
        return Unsafe.getUnsafe().compareAndSwapInt(null, addr + offset, expect, val);
    }

    @Override
    public long getAndAddLong(long offset, long val) {
        return Unsafe.getUnsafe().getAndAddLong(null, addr + offset, val);
    }

    @Override
    public void putByte(long offset, byte v) {
        Unsafe.getUnsafe().putByte(addr + offset, v);
    }

    @Override
    public void putInt(long offset, int v) {
        Unsafe.getUnsafe().putInt(addr + offset, v);
    }

    @Override
    public void putLong(long offset, long v) {
        Unsafe.getUnsafe().putLong(addr + offset, v);
    }

    @Override
    public long addr() {
        return addr;
    }

    @Override
    public void close() {
        Unsafe.getUnsafe().freeMemory(addr);
    }

    @Override
    public XByteBuffer slice(long position) {
        if (position < 0 || position >= this.capacity) {
            throw new XByteBufferException("invalid argument : pos: " + position + "cap: " + capacity);
        }
        return new ByteBufferNative(addr + position, capacity - position);
    }

}
