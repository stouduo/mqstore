package io.openmessaging.service.impl;

import io.openmessaging.model.Index;
import io.openmessaging.service.IndexService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RamIndexService implements IndexService {
    private ConcurrentHashMap<String, List<Index>> indices;

    public RamIndexService() {
        this(slotCount);
    }

    public RamIndexService(int capacity) {
        this.indices = new ConcurrentHashMap<>(capacity);
    }

    @Override
    public void put(String key, Index index) {
        indices.putIfAbsent(key, new ArrayList<>(indexUnitCountPerQueue));
        indices.get(key).add(index);
    }


    @Override
    public List<Index> get(String key, long offset, long num) {
        List<Index> indexList = indices.get(key);
        return indexList.subList((int) offset, Math.min((int) (offset + num), indexList.size()));
    }

    @Override
    public Index get(String key, int index) {
        return indices.get(key).get(index);
    }
}
