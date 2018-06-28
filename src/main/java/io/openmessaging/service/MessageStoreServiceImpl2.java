package io.openmessaging.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStoreServiceImpl2 implements MessageStoreService {
    private MqStoreService mqStoreService;
    private static ConcurrentHashMap<String, List<Index>> queueMap = new ConcurrentHashMap<>();

    public MessageStoreServiceImpl2() {
        this.mqStoreService = new MqStoreService();
    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        List<byte[]> ret = new ArrayList<>();
        List<Index> indices = queueMap.get(queueName);
        for (int i = 0; i < num; i++) {
            int o = (int) (offset + i);
            if (o >= indices.size()) break;
            Index index = indices.get(o);
            ret.add(mqStoreService.get(index.getOffset(), index.getSize()));
        }
        return ret;
    }

    @Override
    public void store(String queueName, byte[] message) {
        queueMap.putIfAbsent(queueName, new ArrayList<>());
        queueMap.get(queueName).add(new Index(message.length, mqStoreService.put(message)));
    }

    public static void main(String[] args) {
        MessageStoreService messageStoreService = new MessageStoreServiceImpl();
        messageStoreService.store("1", "test".getBytes());
        for (byte[] bytes : messageStoreService.get("1", 0, 1)) {
            System.out.println(new String(bytes));
        }
    }

    private static class Index {
        private long offset;
        private int size;

        public Index(int size, long offset) {
            this.offset = offset;
            this.size = size;
        }

        public long getOffset() {
            return offset;
        }

        public int getSize() {
            return size;
        }
    }
}
