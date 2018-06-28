package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.ConsumerQueue;
import io.openmessaging.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public class ConsumerQueueService {
    private List<ConsumerQueue> consumerQueues = new ArrayList<>(10);
    private static int countPerConsumerQueues = Config.countPerConsumerQueues;

    public long[] get(long queueId, int count) {
        byte[] ret = consumerQueues.get((int) queueId / countPerConsumerQueues).get(count);
        long[] retL = {ByteUtil.bytes2Long(ret, 0), ByteUtil.bytes2Long(ret, 8)};
        return retL;
    }

    public int put(long queueId, long offset, int size) {
        int index = (int) queueId / countPerConsumerQueues;
        int createCount = index - consumerQueues.size();
        while (createCount-- >= 0) consumerQueues.add(new ConsumerQueue(consumerQueues.size()));
        ConsumerQueue consumerQueue = consumerQueues.get(index);
        return consumerQueue.put(offset, size);
    }
}
