package io.openmessaging.service.impl;

import io.openmessaging.model.QueueMetaData;
import io.openmessaging.service.IndexService;

public class HashArrayIndexService implements IndexService<Integer> {
    //    private static int delta = 10;
    private static int queueCount = 1010000;
    private long[][] indices = new long[queueCount][200];


    @Override
    public void index(QueueMetaData queueMetaData) {
        indices[queueMetaData.getId()][queueMetaData.getMsgCount() / indexCount] = queueMetaData.getStartOffset();
    }

    @Override
    public QueueMetaData query(QueueMetaData queueMetaData, Integer readIndex) {
        return new QueueMetaData().setMsgCount(readIndex).setStartOffset(indices[queueMetaData.getId()][readIndex / indexCount]);
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
