package com.stouduo.mq.service;

import com.stouduo.mq.config.Config;
import com.stouduo.mq.model.ConsumerQueue;

import java.util.ArrayList;
import java.util.List;

public class ConsumerQueueService {
    private List<ConsumerQueue> consumerQueues = new ArrayList<>(10);
    private static int countPerConsumerQueues = Config.countPerConsumerQueues;

    public long[] get(long queueId, int count) {
        byte[] ret = consumerQueues.get((int) queueId % countPerConsumerQueues).get(count);
        long[] retL = {ret[0] & 0xFF, ret[1] & 0xFF};
        return retL;
    }

    public int put(long queueId, long offset, int size) {
        int index = (int) queueId % countPerConsumerQueues;
        ConsumerQueue consumerQueue = consumerQueues.get(index);
        if (consumerQueue == null) {
            consumerQueue = new ConsumerQueue();
            consumerQueues.add(consumerQueue);
        }
        return consumerQueue.put(offset, size);
    }
}
