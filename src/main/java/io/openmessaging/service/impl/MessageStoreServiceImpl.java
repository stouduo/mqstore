package io.openmessaging.service.impl;

import io.openmessaging.service.MessageStoreService;
import io.openmessaging.service.MqStoreService;
import io.openmessaging.util.ByteUtil;

import java.util.Collection;

public class MessageStoreServiceImpl implements MessageStoreService {
    private MqStoreService mqStoreService;

    public MessageStoreServiceImpl() {
        this.mqStoreService = new MqStoreService();
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        return mqStoreService.get(queueName, offset, (int) num);
    }

    @Override
    public void store(String queueName, byte[] message) {
        mqStoreService.put(queueName, message);
    }

    public static void main(String[] args) {
        MessageStoreService messageStoreService = new MessageStoreServiceImpl();
        messageStoreService.store("11", "test11".getBytes());
        messageStoreService.store("11", "test11".getBytes());
        messageStoreService.store("11", "test11".getBytes());
        messageStoreService.store("22", "test22".getBytes());
        messageStoreService.store("22", "test22".getBytes());
        messageStoreService.store("22", "test22".getBytes());
        messageStoreService.store("33", "test33".getBytes());
        messageStoreService.store("33", "test33".getBytes());
        messageStoreService.store("33", "test33".getBytes());
        messageStoreService.store("44", "test44".getBytes());
        for (byte[] bytes : messageStoreService.get("11", 0, 3)) {
            System.out.println(new String(bytes));
        }
    }

}
