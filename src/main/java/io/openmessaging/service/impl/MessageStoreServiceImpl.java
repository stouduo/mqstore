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
        this.indexService = new RamIndexService();
//        this.indexService = new DiskIndexService();
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        List<byte[]> ret = new ArrayList<>();
        List<Index> indices = indexService.get(queueName, offset, num);
        for (int i = 0; i < indices.size(); i++) {
            Index index = indices.get(i);
            ret.add(mqStoreService.get(index.getOffset(), index.getSize()));
        }
        return ret;
    }

    @Override
    public synchronized void store(String queueName, byte[] message) {
        indexService.put(queueName, new Index(message.length, mqStoreService.put(message)));
    }

    public static void main(String[] args) {
        MessageStoreService messageStoreService = new MessageStoreServiceImpl();
        messageStoreService.store("1", "test".getBytes());
        for (byte[] bytes : messageStoreService.get("1", 0, 1)) {
            System.out.println(new String(bytes));
        }
    }

}
