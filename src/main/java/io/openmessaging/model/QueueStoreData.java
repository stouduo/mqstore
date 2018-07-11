package io.openmessaging.model;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void updateSize() {
        size += 1;
    }

    public void index(int size, long offset) {
        indices[size / indexCount - 1] = offset;
    }

    public long query(int key) {
        return indices[key];
    }

}
