package io.openmessaging.service;

import io.openmessaging.model.QueueStoreFlag;

public interface IndexService<T extends Comparable> {
    void index(String queue, QueueStoreFlag flag);

    QueueStoreFlag query(String queue, T key);
}
