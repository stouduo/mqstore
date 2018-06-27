package com.stouduo.mq;

import com.stouduo.mq.service.MessageStoreService;
import com.stouduo.mq.service.MessageStoreServiceImpl;

import java.util.Collection;

public class CustomQueueStoreImpl extends QueueStore {
    private MessageStoreService messageStoreService;

    public CustomQueueStoreImpl() {
        this.messageStoreService = new MessageStoreServiceImpl();
    }

    @Override
    public void put(String queueName, byte[] message) {
        messageStoreService.store(queueName, message);
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        return messageStoreService.get(queueName, offset, num);
    }
}
