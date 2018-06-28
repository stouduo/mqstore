package io.openmessaging.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class MessageStoreServiceImpl implements MessageStoreService {
    private MqStoreService mqStoreService;
    private ConsumerQueueService consumerQueueService;
    private static ConcurrentHashMap<String, List<Integer>> queueMap = new ConcurrentHashMap<>();

    private static AtomicLong queueIdGenerator = new AtomicLong(0);
    private static ConcurrentHashMap<String, Long> queueIds = new ConcurrentHashMap<>();
    private ReentrantLock lock = new ReentrantLock();

    public MessageStoreServiceImpl() {
        this.mqStoreService = new MqStoreService();
        this.consumerQueueService = new ConsumerQueueService();
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        List<byte[]> ret = new ArrayList<>();
        List<Integer> indices = queueMap.get(queueName);
        for (int index = 0; index < num; index++) {
            int o = (int) (offset + index);
            if (o >= indices.size()) break;
            long[] result = consumerQueueService.get(queueIds.get(queueName), indices.get(o));
            ret.add(mqStoreService.get(result[0], (int) result[1]));
        }
        return ret;
    }

    @Override
    public void store(String queueName, byte[] message) {
        queueIds.putIfAbsent(queueName, queueIdGenerator.getAndIncrement());
        queueMap.putIfAbsent(queueName, new ArrayList<>());
        long id = queueIds.get(queueName);
        long storeOffset = mqStoreService.put(message);
        queueMap.get(queueName).add(consumerQueueService.put(id, storeOffset, message.length));
    }

    public static void main(String[] args) {
        MessageStoreService messageStoreService = new MessageStoreServiceImpl();
        messageStoreService.store("1", "test".getBytes());
        for (byte[] bytes : messageStoreService.get("1", 0, 1)) {
            System.out.println(new String(bytes));
        }
    }
}
