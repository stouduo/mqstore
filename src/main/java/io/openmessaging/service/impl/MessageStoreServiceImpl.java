package io.openmessaging.service.impl;

import io.openmessaging.model.Index;
import io.openmessaging.service.IndexService;
import io.openmessaging.service.MessageStoreService;
import io.openmessaging.service.MqStoreService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStoreServiceImpl implements MessageStoreService {
    private MqStoreService mqStoreService;
    private IndexService indexService;

    public MessageStoreServiceImpl() {
        this.mqStoreService = new MqStoreService();
//        this.indexService = new RamIndexService();
//        this.indexService = new DiskIndexService();
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
        messageStoreService.store("13421", "test234".getBytes());
        for (byte[] bytes : messageStoreService.get("13421", 0, 1)) {
            System.out.println(new String(bytes));
        }
    }

}
