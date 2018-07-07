package io.openmessaging.service.impl;

import io.openmessaging.model.QueueStoreFlag;
import io.openmessaging.service.IndexService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HashArrayIndexService implements IndexService<Integer> {
    //    private static int delta = 10;
    private static int queueCount = 1010000;
    private long[][] indices = new long[queueCount][200];
    private static Map<String, Integer> queueIds = new HashMap<>();
    private static AtomicInteger idGene = new AtomicInteger(0);


    @Override
    public void index(String queue, QueueStoreFlag flag) {
        queueIds.putIfAbsent(queue, idGene.getAndIncrement());
        indices[queueIds.get(queue)][flag.getSize() / indexCount - 1] = flag.getLastOffset();
    }

    @Override
    public QueueStoreFlag query(String queue, Integer key) {
        return new QueueStoreFlag(indices[queueIds.get(queue)][key / indexCount - 1], key);
    }

//    public int arrayIndex(String queue) {
//        int keyHashPositive = Math.abs(queue.hashCode());
//        if (keyHashPositive < 0)
//            keyHashPositive = 0;
//        return keyHashPositive % queueCount;
//    }

//    public void ensureCapacity(int index) {
//
//    }
}
