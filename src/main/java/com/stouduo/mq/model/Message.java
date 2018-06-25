package com.stouduo.mq.model;

import java.util.concurrent.atomic.AtomicLong;

public class Message {
    private static AtomicLong idGenerator = new AtomicLong(0);
    private long id;
    private String msgQueue;
    private String body;

    public Message() {
        this.id = idGenerator.getAndIncrement();
    }

    public Message(String msgQueue, String body) {
        this();
        this.msgQueue = msgQueue;
        this.body = body;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getMsgQueue() {
        return msgQueue;
    }

    public void setMsgQueue(String msgQueue) {
        this.msgQueue = msgQueue;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
