package io.openmessaging.service.impl;

import io.openmessaging.model.QueueStoreFlag;
import io.openmessaging.service.IndexService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashMapIndexService implements IndexService<Integer> {
    private Map<String, List<Long>> indices = new ConcurrentHashMap<>();

    @Override
    public void index(String queue, QueueStoreFlag flag) {
        indices.putIfAbsent(queue, new ArrayList<>());
        indices.get(queue).add(flag.getLastOffset());
    }

    @Override
    public QueueStoreFlag query(String queue, Integer key) {
        return new QueueStoreFlag(indices.get(queue).get(key / indexCount - 1), key);
    }
}
