package io.openmessaging.model;

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
        this.dirtyData = ByteBuffer.allocateDirect(0);
    }

    public int getId() {
        return id;
    }

    public ByteBuffer getDirtyData() {
        return dirtyData;
    }

    public QueueStoreData setDirtyData(byte[] dirtyData) {
        ByteBuffer dirty = ByteBuffer.allocateDirect(this.dirtyData.capacity() + dirtyData.length);
        dirty.put(this.dirtyData);
        dirty.put(dirtyData);
        this.dirtyData.clear();
        this.dirtyData = dirty;
        this.dirtyData.position(0);
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
