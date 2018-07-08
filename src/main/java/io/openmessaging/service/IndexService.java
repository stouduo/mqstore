package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.QueueMetaData;

public interface IndexService<T extends Comparable> {
    int indexCount = Config.indexCount;

    void index(QueueMetaData queueMetaData);

    QueueMetaData query(QueueMetaData queueMetaData, T key);
}
