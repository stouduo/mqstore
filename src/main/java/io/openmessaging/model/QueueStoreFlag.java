package io.openmessaging.model;

public class QueueStoreFlag implements Cloneable {
    private long lastOffset;
    private int size;

    public QueueStoreFlag(long lastOffset, int size) {
        this.lastOffset = lastOffset;
        this.size = size;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public int getSize() {
        return size;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void updateOffset(long logicOffset) {
        setLastOffset(logicOffset);
    }

    public void updateSize() {
        size += 1;
    }

    public QueueStoreFlag clone() {
        try {
            return (QueueStoreFlag) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
