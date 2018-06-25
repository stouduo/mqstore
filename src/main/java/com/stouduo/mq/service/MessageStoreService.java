package com.stouduo.mq.service;

import com.stouduo.mq.model.Message;

import java.util.Collection;

public interface MessageStoreService {
    Collection<String> get(String queueName, long offset, long num);

    void store(Message message);
}
