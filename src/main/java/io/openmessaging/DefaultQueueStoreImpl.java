package io.openmessaging;

import io.openmessaging.service.MessageStoreService;
import io.openmessaging.service.impl.MessageStoreServiceImpl;

import java.util.Collection;

/**
 * 这是一个简单的基于文件的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {
    private MessageStoreService messageStoreService;

    public DefaultQueueStoreImpl() {
        this.messageStoreService = new MessageStoreServiceImpl();
    }

    @Override
    public void put(String queueName, byte[] message) {
        messageStoreService.store(queueName, message);
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        return messageStoreService.get(queueName, offset, num);
    }
}