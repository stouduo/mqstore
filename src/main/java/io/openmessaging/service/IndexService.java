package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.Index;

import java.util.List;

public interface IndexService {
    int indexNum = Config.indexNum;
    int capacity = Config.capacity;

    default boolean isAsync() {
        return true;
    }

    default int hashKey(String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    void put(String key, Index index);

    public List<Index> get(String key);
}
