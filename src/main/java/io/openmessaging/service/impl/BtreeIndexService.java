package io.openmessaging.service.impl;

import io.openmessaging.index.bplustree.BplusTree;
import io.openmessaging.model.QueueMetaData;
import io.openmessaging.service.IndexService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BtreeIndexService implements IndexService<Integer> {
    private static Map<Integer, BplusTree<QueueMetaData>> indexs = new ConcurrentHashMap<>();

    @Override
    public void index(QueueMetaData flag) {
        indexs.putIfAbsent(flag.getId(), new BplusTree<>(6));
        indexs.get(flag.getId()).insertOrUpdate(flag.getMsgCount(), flag);
    }

    @Override
    public QueueMetaData query(QueueMetaData metaData, Integer key) {
        BplusTree<QueueMetaData> index = indexs.get(metaData.getId());
        QueueMetaData flag = null;
        while (flag == null && !index.keyOutOfBounds(key)) {
            flag = index.get(key++);
        }
        if (flag != null) return flag;
        if (index.keyOutOfBounds(key)) return index.getMaxOne();
        return null;
    }
}
