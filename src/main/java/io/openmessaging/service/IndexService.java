package io.openmessaging.service;

import io.openmessaging.config.Config;

public interface IndexService<T extends Comparable> {
    int indexCount = Config.indexCount;
    int queueCount = 1000000;
    int idxCount = 200;

    long query(int queue, T key);

    void index(int queue, int size, long logicOffset);
}
