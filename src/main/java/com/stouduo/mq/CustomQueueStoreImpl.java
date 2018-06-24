package com.stouduo.mq;

import java.util.Collection;

public class CustomQueueStoreImpl extends QueueStore {
    @Override
    public void put(String queueName, String message) {

    }

    @Override
    public Collection<String> get(String queueName, long offset, long num) {
        return null;
    }
}
