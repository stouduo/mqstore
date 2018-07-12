package io.openmessaging.model;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class QueueStoreData {
    private volatile int size;
    private ByteBuffer dirtyData;
    private long[] indices;
    private static int indexCount = 10;

    public QueueStoreData() {
        this.size = 0;
        this.dirtyData = ByteBuffer.allocateDirect(1024);
        this.indices = new long[200];
    }

    public void clear() {
        ((DirectBuffer) dirtyData).cleaner().clean();
    }

    public ByteBuffer getDirtyData() {
        return dirtyData;
    }

    public QueueStoreData putDirtyData(int dataLen) {
        this.dirtyData.putInt(dataLen);
        return this;
    }

    public QueueStoreData putDirtyData(byte[] dirtyData) {
        this.dirtyData.put(dirtyData);
        return this;
    }

    public QueueStoreData fillDirtyData(int len) {
        dirtyData.position(dirtyData.position() + len);
        return this;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int updateSize() {
        return ++size;
    }

    public void index(int size, long offset) {
        indices[size / indexCount - 1] = offset;
    }

    public long query(int key) {
        return indices[key];
    }

}
