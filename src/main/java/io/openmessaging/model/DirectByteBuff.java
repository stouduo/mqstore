package io.openmessaging.model;

import sun.misc.Unsafe;


public class DirectByteBuff extends Buffer {
    private long address;
    private static Unsafe unsafe = Unsafe.getUnsafe();

    public DirectByteBuff(int mark, int pos, int lim, int cap) {
        super(mark, pos, lim, cap);
    }

    public DirectByteBuff(int cap) {
        this(-1, 0, cap, cap);
        this.address = unsafe.allocateMemory(cap);
        unsafe.setMemory(address, cap, (byte) 0);
    }

    public void putInt(int i) {
        putInt(position(), i);
    }

    public void putInt(int position, int i) {
        unsafe.putInt(position, i);
        position(position + 4);
    }

    public void put(byte[] bytes) {
        put(position(), bytes);
    }

    public void put(int position, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            unsafe.putByte(position + i, bytes[i]);
        }
        position(position + bytes.length);
    }

    public int readInt(int position) {
        return unsafe.getInt(address + position);
    }

    public long readLong(int position) {
        return unsafe.getLong(address + position);
    }


    public void free() {
        unsafe.freeMemory(address);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public Object array() {
        return null;
    }

    @Override
    public int arrayOffset() {
        return 0;
    }

    @Override
    public boolean isDirect() {
        return true;
    }
}
