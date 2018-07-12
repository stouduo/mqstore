package io.openmessaging.model;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueStoreData {
    private AtomicInteger size;
    private ByteBuffer dirtyData;
    private long[] indices;
    private static int indexCount = 20;
    private static ByteBuffer buff = ByteBuffer.allocateDirect((int) (1024 * 1024 * 1024 * 1.2));

    public QueueStoreData() {
        this.size = new AtomicInteger(0);
        buff.position(size.get() * 1280).limit(1280);
        this.dirtyData = buff.slice();
        this.indices = new long[100];
    }

    public void clear() {
//        ((DirectBuffer) dirtyData).cleaner().clean();
        ((DirectBuffer) buff).cleaner().clean();
    }

    public ByteBuffer getDirtyData() {
        return dirtyData;
    }

    public QueueStoreData putDirtyData(int dataLen) {
        this.dirtyData.putInt(dataLen);
        return this;
    }

    public QueueStoreData putDirtyData(int len, byte[] data, int fillLen) {
        this.dirtyData.putInt(len).put(data).position(dirtyData.position() + fillLen);
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
        return size.get();
    }

    public int updateSize() {
        return size.incrementAndGet();
    }

    public void index(int size, long offset) {
        indices[size / indexCount - 1] = offset;
    }

    public long query(int key) {
        return indices[key];
    }

}
