package io.openmessaging.service.impl;

import io.openmessaging.service.IndexService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RamIndexService implements IndexService {
    private ConcurrentHashMap<String, List<long[]>> indices;

    public RamIndexService() {
        this(slotCount);
    }

    public RamIndexService(int capacity) {
        this.indices = new ConcurrentHashMap<>(capacity);
    }

    @Override
    public void put(String key, long offset, int size) {
        indices.putIfAbsent(key, new ArrayList<>());
        indices.get(key).add(new long[]{offset, size});
    }


    @Override
    public List<long[]> get(String key, long offset, long num) {
        List<long[]> indexList = indices.get(key);
        return indexList.subList((int) offset, Math.min((int) (offset + num), indexList.size()));
    }

    @Override
    public long[] get(String key, int index) {
        return indices.get(key).get(index);
    }
}
