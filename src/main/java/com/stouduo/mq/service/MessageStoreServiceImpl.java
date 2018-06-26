package com.stouduo.mq.service;

import com.stouduo.mq.util.StringCompress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class MessageStoreServiceImpl implements MessageStoreService {
    private MqStoreService mqStoreService;
    private ConsumerQueueService consumerQueueService;
    private static ConcurrentHashMap<String, ArrayList<Integer>> queueMap = new ConcurrentHashMap<>();
    private static ThreadPoolExecutor worker;
    private static String threadNamePrefix = "mq-ioWorker-";
    private static int ioThreadIndex = 0;

    private static AtomicLong queueIdGenerator = new AtomicLong(0);
    private static ConcurrentHashMap<String, Long> queueIds = new ConcurrentHashMap<>();

    public MessageStoreServiceImpl() {
        this.mqStoreService = new MqStoreService();
        this.consumerQueueService = new ConsumerQueueService();
//        this.worker = new ThreadPoolExecutor(10, 200, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100),
//                r -> {
//                    Thread thread = new Thread(r);
//                    thread.setName(threadNamePrefix + ioThreadIndex++);
//                    return thread;
//                }, new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    @Override
    public Collection<String> get(String queueName, long offset, long num) {
        List<String> ret = new ArrayList<>();
        for (int index = 0; index < num; index++) {
            long[] result = consumerQueueService.get(queueIds.get(queueName), queueMap.get(queueName).get((int) (offset + index)));
            ret.add(StringCompress.decompress(mqStoreService.get(result[0], (int) result[1])));
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
//        worker.execute(() -> {
            byte[] body = StringCompress.compress(message);
            long storeOffset = mqStoreService.put(body);
            ArrayList<Integer> indices = queueMap.get(queueName);
            if (indices == null) {
                indices = new ArrayList<>();
                queueMap.put(queueName, indices);
            }
            indices.add(consumerQueueService.put(id, storeOffset, body.length));
//        });
    }
}
