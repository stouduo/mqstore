package io.openmessaging.service.impl;

import io.openmessaging.model.QueueMetaData;
import io.openmessaging.service.IndexService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashMapIndexService implements IndexService<Integer> {
    private Map<Integer, List<Long>> indices = new ConcurrentHashMap<>();

    @Override
    public void index(QueueMetaData queueMetaData) {
        indices.putIfAbsent(queueMetaData.getId(), new ArrayList<>());
        indices.get(queueMetaData.getId()).add(queueMetaData.getEndOffset());
    }

    @Override
    public QueueMetaData query(QueueMetaData queueMetaData, Integer key) {
        return new QueueMetaData().setEndOffset(indices.get(queueMetaData.getId()).get(key / indexCount - 1));
    }
}
