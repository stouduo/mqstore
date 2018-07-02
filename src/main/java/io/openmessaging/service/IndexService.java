package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.Index;

import java.util.Collection;
import java.util.List;

public interface IndexService {
    int indexUnitCountPerQueue = Config.indexUnitCountPerQueue;
    int slotCount = Config.slotCount;

    void put(String key, long offset, int size);

    List<long[]> get(String key, long offset, long num);

    long[] get(String key, int index);
}
