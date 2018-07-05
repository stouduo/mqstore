package io.openmessaging.service.impl;

import io.openmessaging.model.QueueStoreFlag;
import io.openmessaging.service.IndexService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashIndexService implements IndexService<Integer> {
    private Map<String, Map<Integer, QueueStoreFlag>> indices = new ConcurrentHashMap<>();

    @Override
    public void index(String queue, QueueStoreFlag flag) {
        indices.putIfAbsent(queue, new HashMap<>());
        indices.get(queue).put(flag.getSize(), flag);
    }

    @Override
    public QueueStoreFlag query(String queue, Integer key) {
        return indices.get(queue).get(key);
    }
}
