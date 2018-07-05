package io.openmessaging.service;

import io.openmessaging.index.bplustree.BplusTree;
import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.model.QueueStoreFlag;
import io.openmessaging.service.impl.BtreeIndexService;
import io.openmessaging.service.impl.HashIndexService;
import io.openmessaging.util.ByteUtil;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MqStoreService implements IndexService<Integer> {
    private static int storeFileSize = Config.mqStoreFileSize;
    private List<MappedFile> storeFiles = new ArrayList<>(10);
    private static AtomicInteger fileNameIndex = new AtomicInteger(0);
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static long logicOffset = 0;
    private static Map<String, QueueStoreFlag> storeFlags = new ConcurrentHashMap<>();
    private IndexService indexService;
    private static int indexCount = Config.indexCount;

    public MqStoreService() {
//        indexService = new BtreeIndexService();
//        indexService = this;
        indexService = new HashIndexService();
    }

    private MappedFile create() {
        return new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize).setFileFlushSize(50 * 1024 * 1024);
    }

    public synchronized void put(String queueName, byte[] message) {
        storeFlags.putIfAbsent(queueName, new QueueStoreFlag(-1, 0));
        QueueStoreFlag flag = storeFlags.get(queueName);
        flag.updateSize();
        long lastOffset = flag.getLastOffset();
        MappedFile writableFile;
        long fileOffset = logicOffset % storeFileSize;
        if (fileOffset == 0) {
            storeFiles.add(create());
        }
        writableFile = storeFiles.get(storeFiles.size() - 1);
        if (offsetOutOfBound(fileOffset, 12 + message.length)) {
            writableFile = create();
            storeFiles.add(writableFile);
            logicOffset += storeFileSize - fileOffset;
        }
        flag.updateOffset(logicOffset);
        if (flag.getSize() % indexCount == 0) {
            indexService.index(queueName, flag.clone());
        }
        writableFile.writeLong(lastOffset);
        writableFile.writeInt(message.length);
        writableFile.appendData(message);
        logicOffset += 12 + message.length;
    }

    public List<byte[]> get(String queueName, long offset, int num) {
        LinkedList<byte[]> msgs = new LinkedList<>();
        QueueStoreFlag flag = storeFlags.get(queueName);
        flag = getNearestOffset(queueName, (int) offset + num, flag);
        long preOffset = flag.getLastOffset();
        int i, msgLen;
        for (i = flag.getSize(); i > offset + num; i--) {
            preOffset = getPreOffset(preOffset);
        }
        for (; i > offset; i--) {
            msgLen = getMsgLength(preOffset + 8);
            msgs.addFirst(ByteUtil.uncompress(getMsg(preOffset + 12, msgLen)));
            preOffset = getPreOffset(preOffset);
        }
        return msgs;
    }

    private long getPreOffset(long offset) {
        return getActualFile(offset).getLong((int) (offset % storeFileSize));
    }

    private QueueStoreFlag getNearestOffset(String queue, int readIndex, QueueStoreFlag lastFlag) {
        if (readIndex >= lastFlag.getSize()) return lastFlag;
        int index = 0;
        while (index < readIndex) index += indexCount;
        return indexService.query(queue, index);
    }

    private byte[] getMsg(long offset, int len) {
        byte[] msg = new byte[len];
        byteBuff2bytes(getActualFile(offset).read((int) (offset % storeFileSize), len), 0, msg);
        return msg;
    }

    private int getMsgLength(long offset) {
        return getActualFile(offset).getInt((int) (offset % storeFileSize));
    }

    private MappedFile getActualFile(long offset) {
        return storeFiles.get((int) (offset / storeFileSize));
    }

    public static void byteBuff2bytes(ByteBuffer byteBuffer, int offset, byte[] ret) {
        if (offset == -1) offset = byteBuffer.position();
        byteBuffer.get(ret, offset, byteBuffer.limit() - byteBuffer.position());
    }

    private boolean offsetOutOfBound(long offset, int size) {
        return offset < storeFileSize && offset + size > storeFileSize;
    }

    @Override
    public void index(String queue, QueueStoreFlag flag) {
    }

    @Override
    public QueueStoreFlag query(String queue, Integer key) {
        return storeFlags.get(queue);
    }
}
