package io.openmessaging.model;

import io.openmessaging.util.ByteUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueStoreData {
    private static AtomicInteger idGene = new AtomicInteger(0);
    private int id;
    private volatile int size;
    private ByteBuffer dirtyData;

    public QueueStoreData() {
        this.id = idGene.getAndIncrement();
        this.size = 0;
        this.dirtyData = ByteBuffer.allocate(1536);
    }

    public int getId() {
        return id;
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

}
