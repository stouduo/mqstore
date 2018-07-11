package io.openmessaging.model;

import java.util.concurrent.atomic.AtomicInteger;

public class QueueStoreData {
    private volatile int msgIndex;
    private static AtomicInteger idGene = new AtomicInteger(0);
    private int id;

    public QueueStoreData() {
        this.msgIndex = 0;
        this.id = idGene.getAndIncrement();
    }

    public int getId() {
        return id;
    }

    public int getMsgIndex() {
        return msgIndex;
    }

    public void setMsgIndex(int msgIndex) {
        this.msgIndex = msgIndex;
    }

    public int updateMsgIndex() {
        return msgIndex++;
    }
}
