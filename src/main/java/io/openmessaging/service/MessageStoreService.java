package io.openmessaging.service;

import java.util.Collection;

public interface MessageStoreService {
    Collection<byte[]> get(String queueName, long offset, long num);

    void store(String queueName, byte[] message);
}
