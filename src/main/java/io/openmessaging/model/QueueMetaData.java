package io.openmessaging.model;

import java.util.concurrent.atomic.AtomicInteger;

public class QueueMetaData {
    private int id;
    private static AtomicInteger idGene = new AtomicInteger(0);
    private volatile long startOffset = 0;
    private volatile long endOffset = 0;
    private volatile int msgCount = 0;
    private int fileIndex;

    public QueueMetaData() {
    }

    public QueueMetaData(int blockCountPerFile, int blockSize) {
        this.id = idGene.getAndIncrement();
        this.fileIndex = id / blockCountPerFile;
        this.startOffset = id * blockSize - fileIndex * blockCountPerFile * blockSize;
        this.endOffset = this.startOffset;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public void setFileIndex(int fileIndex) {
        this.fileIndex = fileIndex;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public QueueMetaData setStartOffset(long startOffset) {
        this.startOffset = startOffset;
        return this;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public QueueMetaData setEndOffset(long endOffset) {
        this.endOffset = endOffset;
        return this;
    }

    public int getMsgCount() {
        return msgCount;
    }

    public QueueMetaData setMsgCount(int msgCount) {
        this.msgCount = msgCount;
        return this;
    }

    public void updateMsgCount() {
        msgCount += 1;
    }

    public void updateEndOffset(int delta) {
        endOffset += delta;
    }
}
