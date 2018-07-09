package io.openmessaging.model;

import io.openmessaging.util.ByteUtil;

import java.util.concurrent.atomic.AtomicInteger;

public class QueueStoreData {
    private static AtomicInteger idGene = new AtomicInteger(0);
    private int id;
    private volatile int size;
    private byte[] dirtyData;

    public QueueStoreData() {
        this.id = idGene.getAndIncrement();
        this.size = 0;
        this.dirtyData = new byte[0];
    }

    public int getId() {
        return id;
    }

    public byte[] getDirtyData() {
        byte[] dirtyData = this.dirtyData;
        this.dirtyData = new byte[0];
        return dirtyData;
    }

    public QueueStoreData setDirtyData(byte[] dirtyData) {
        byte[] dirty = new byte[this.dirtyData.length + dirtyData.length];
        ByteUtil.byteMerger(dirty, this.dirtyData, dirtyData);
        this.dirtyData = dirty;
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
