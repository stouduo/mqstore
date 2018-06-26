package com.stouduo.mq.model;

import com.stouduo.mq.config.Config;
import com.stouduo.mq.service.MqStoreService;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerQueue {
    private static int queueUnitCount = Config.consumerStoreUnitCount;
    private static int queueUnitSize = Config.CONSUMER_QUEUE_STORE_UNIT_SIZE;
    private static int queueFileSize = queueUnitSize * queueUnitCount;
    private static String queueFileStorePath = Config.rootPath + Config.consumerStorePath;
    private List<MappedFile> queues = new ArrayList<>(10);
    private AtomicInteger logicUnitCount = new AtomicInteger(0);
    private String consumerQueueIndex;

    public ConsumerQueue(int consumerQueueIndex) {
        this.consumerQueueIndex = consumerQueueIndex + "";
        init();
    }

    private void init() {
        queues.add(create());
    }

    private MappedFile create() {
        MappedFile file = new MappedFile(MessageFormat.format("consumer_queue_{0}-{1}.data", consumerQueueIndex, queues.size() + ""), queueFileStorePath, queueFileSize);
        file.boundChannelToByteBuffer();
        return file;
    }

    public synchronized int put(long offset, int size) {
        MappedFile writableFile = queues.get(queues.size() - 1);
        int retCount = logicUnitCount.get();
        try {
            if (retCount % queueUnitCount == 0) {
                writableFile = create();
                queues.add(writableFile);
            }
            writableFile.appendData(new byte[]{(byte) offset, (byte) size});
            logicUnitCount.getAndIncrement();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retCount;
    }

    public byte[] get(int count) {
        byte[] ret = new byte[12];
        int logicCount = logicUnitCount.get();
        MqStoreService.byteBuff2bytes(queues.get(count / logicCount).read(count % logicCount * queueUnitSize, 12), ret);
        return ret;
    }
}
