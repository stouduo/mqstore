package com.stouduo.mq.service;

import java.util.Collection;

public interface MessageStoreService {
    Collection<String> get(String queueName, long offset, long num);

    void store(String queueName, String message);
}
