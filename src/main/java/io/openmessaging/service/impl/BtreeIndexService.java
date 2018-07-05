package io.openmessaging.service.impl;

import io.openmessaging.index.bplustree.BplusTree;
import io.openmessaging.model.QueueStoreFlag;
import io.openmessaging.service.IndexService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BtreeIndexService implements IndexService<Integer> {
    private static Map<String, BplusTree<QueueStoreFlag>> indexs = new ConcurrentHashMap<>();

    @Override
    public void index(String queue, QueueStoreFlag flag) {
        indexs.putIfAbsent(queue, new BplusTree<>(6));
        indexs.get(queue).insertOrUpdate(flag.getSize(), flag);
    }

    @Override
    public QueueStoreFlag query(String queue, Integer key) {
        BplusTree<QueueStoreFlag> index = indexs.get(queue);
        QueueStoreFlag flag = null;
        while (flag == null && !index.keyOutOfBounds(key)) {
            flag = index.get(key++);
        }
        if (flag != null) return flag;
        if (index.keyOutOfBounds(key)) return index.getMaxOne();
        return null;
    }
}
