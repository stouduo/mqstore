package io.openmessaging.model;

public class Index {
    private long offset;
    private int size;

    public Index(int size, long offset) {
        this.offset = offset;
        this.size = size;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }
}