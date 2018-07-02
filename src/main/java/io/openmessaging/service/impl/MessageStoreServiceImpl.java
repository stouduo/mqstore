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
        List<long[]> indices = indexService.get(queueName, offset, num);
        for (int i = 0; i < indices.size(); i++) {
            long[] index = indices.get(i);
            ret.add(mqStoreService.get(index[0], (int) index[1]));
        }
        return ret;
    }

    @Override
    public void store(String queueName, byte[] message) {
        indexService.put(queueName, mqStoreService.put(message), message.length);
    }

    public static void main(String[] args) {
        MessageStoreService messageStoreService = new MessageStoreServiceImpl();
        messageStoreService.store("1", "test".getBytes());
        for (byte[] bytes : messageStoreService.get("1", 0, 1)) {
            System.out.println(new String(bytes));
        }
    }

}
