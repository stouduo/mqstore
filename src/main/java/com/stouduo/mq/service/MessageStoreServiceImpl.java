package com.stouduo.mq.service;

import com.stouduo.mq.model.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MessageStoreServiceImpl implements MessageStoreService {
    private MqStoreService mqStoreService;
    private ConsumerQueueService consumerQueueService;
    private static ConcurrentHashMap<Long, ArrayList<Integer>> queueMap = new ConcurrentHashMap<>();
    private static ThreadPoolExecutor worker;
    private static String threadNamePrefix = "mq-cq-worker-";

    public MessageStoreServiceImpl() {
        this.mqStoreService = new MqStoreService();
        this.consumerQueueService = new ConsumerQueueService();
        this.worker = new ThreadPoolExecutor(2, 4, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName(threadNamePrefix + thread.getId());
                    return thread;
                }, new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    @Override
    public Collection<String> get(String queueName, long offset, long num) {
        return null;
    }

    @Override
    public void store(Message message) {

    }
}
