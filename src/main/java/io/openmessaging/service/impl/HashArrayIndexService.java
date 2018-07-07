package io.openmessaging.service.impl;

import io.openmessaging.model.QueueStoreFlag;
import io.openmessaging.service.IndexService;

public class HashArrayIndexService implements IndexService<Integer> {
    //    private static int delta = 10;
    private static int queueCount = 1010000;
    private long[][] indices = new long[queueCount][200];


    @Override
    public void index(String queue, QueueStoreFlag flag) {
        indices[arrayIndex(queue)][flag.getSize() / indexCount - 1] = flag.getLastOffset();
    }

    @Override
    public QueueStoreFlag query(String queue, Integer key) {
        return new QueueStoreFlag(indices[arrayIndex(queue)][key / indexCount - 1], key);
    }

    public int arrayIndex(String queue) {
        int keyHashPositive = Math.abs(queue.hashCode());
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive % queueCount;
    }

//    public void ensureCapacity(int index) {
//
//    }
}
