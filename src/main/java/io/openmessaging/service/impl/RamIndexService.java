package io.openmessaging.service.impl;

import io.openmessaging.model.Index;
import io.openmessaging.service.IndexService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RamIndexService implements IndexService {
    private ConcurrentHashMap<String, List<Index>> indices;

    public RamIndexService() {
        this(capacity);
    }

    public RamIndexService(int capacity) {
        this.indices = new ConcurrentHashMap<>(capacity);
    }

    @Override
    public void put(String key, Index index) {
        indices.putIfAbsent(key, new ArrayList<>(indexNum));
        indices.get(key).add(index);
    }

    @Override
    public List<Index> get(String key) {
        return indices.get(key);
    }
}
