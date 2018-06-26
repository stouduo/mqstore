package com.stouduo.mq.service;

import com.stouduo.mq.util.StringCompress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MessageStoreServiceImpl implements MessageStoreService {
    private MqStoreService mqStoreService;
    private ConsumerQueueService consumerQueueService;
    private static ConcurrentHashMap<String, ArrayList<Integer>> queueMap = new ConcurrentHashMap<>();

    private static AtomicLong queueIdGenerator = new AtomicLong(0);
    private static ConcurrentHashMap<String, Long> queueIds = new ConcurrentHashMap<>();

    public MessageStoreServiceImpl() {
        this.mqStoreService = new MqStoreService();
        this.consumerQueueService = new ConsumerQueueService();
    }

    @Override
    public synchronized Collection<String> get(String queueName, long offset, long num) {
        List<String> ret = new ArrayList<>();
        for (int index = 0; index < num; index++) {
           int o = (int) (offset + index);
            System.out.println(o +"--"+queueMap.get(queueName).size());
            long[] result = consumerQueueService.get(queueIds.get(queueName), queueMap.get(queueName).get(o));
            ret.add(new String(mqStoreService.get(result[0], (int) result[1])));
        }
        return ret;
    }

    @Override
    public synchronized void store(String queueName, String message) {
        Long queueId = queueIds.get(queueName);
        if (queueId == null) {
            queueIds.put(queueName, queueIdGenerator.getAndIncrement());
        }
        final long id = queueIds.get(queueName);
        byte[] body = message.getBytes();
        long storeOffset = mqStoreService.put(body);
        ArrayList<Integer> indices = queueMap.get(queueName);
        if (indices == null) {
            indices = new ArrayList<>();
            queueMap.put(queueName, indices);
        }
        indices.add(consumerQueueService.put(id, storeOffset, body.length));
    }

    public static void main(String[] args) {
        MessageStoreService messageStoreService = new MessageStoreServiceImpl();
        messageStoreService.store("1", "test");
        System.out.println(messageStoreService.get("1", 0, 1));
    }
}
