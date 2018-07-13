package io.openmessaging.model;

import io.openmessaging.config.Config;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueueStoreData {
    private AtomicInteger size;
    private ByteBuffer dirtyData;
    private long[] indices;
    private static int indexCount = Config.indexCount;
    private static int count = 0;

    public QueueStoreData() {
        this.size = new AtomicInteger(0);
        if (count++ % 4 == 0) {
            this.dirtyData = ByteBuffer.allocate(2048);
        } else {
            this.dirtyData = ByteBuffer.allocateDirect(2048);
        }
        this.indices = new long[32];
    }

    public void clear() {
        if (dirtyData instanceof DirectBuffer)
            ((DirectBuffer) dirtyData).cleaner().clean();
//        ((DirectBuffer) directBuff).cleaner().clean();
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
