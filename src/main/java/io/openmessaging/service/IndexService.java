package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.QueueStoreFlag;

public interface IndexService<T extends Comparable> {
    int indexCount = Config.indexCount;
    void index(String queue, QueueStoreFlag flag);

    QueueStoreFlag query(String queue, T key);
}
