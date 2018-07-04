package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.model.QueueStoreFlag;
import io.openmessaging.util.ByteUtil;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MqStoreService {
    private static int storeFileSize = Config.mqStoreFileSize;
    private List<MappedFile> storeFiles = new ArrayList<>(10);
    private static AtomicInteger fileNameIndex = new AtomicInteger(0);
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static long logicOffset = 0;
    private static Map<String, QueueStoreFlag> storeFlags = new ConcurrentHashMap<>();

    public MqStoreService() {
    }

    private MappedFile create() {
        return new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize).setFileFlushSize(50 * 1024 * 1024);
    }

    public synchronized void put(String queueName, byte[] message) {
        storeFlags.putIfAbsent(queueName, new QueueStoreFlag(0, 0));
        QueueStoreFlag flag = storeFlags.get(queueName);
        long lastOffset = flag.getLastOffset();
        flag.updateOffset(logicOffset);
        flag.updateSize();
        long fileOffset = logicOffset % storeFileSize;
        MappedFile writableFile;
        try {
            if (fileOffset == 0) {
                storeFiles.add(create());
            }
            writableFile = storeFiles.get(storeFiles.size() - 1);
            if (offsetOutOfBound(fileOffset, 12 + message.length)) {
                writableFile = create();
                storeFiles.add(writableFile);
            }
            byte[] head = new byte[12];
            ByteUtil.long2Bytes(head, 0, lastOffset);
            ByteUtil.int2Bytes(head, 8, message.length);
            byte[] data = new byte[head.length + message.length];
            ByteUtil.byteMerger(data, head, message);
            writableFile.appendData(data);
            logicOffset += data.length;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<byte[]> get(String queueName, long offset, int num) {
        List<byte[]> msgs = new ArrayList<>(num);
        QueueStoreFlag flag = storeFlags.get(queueName);
        long lastOffset = flag.getLastOffset();
        int msgSize = flag.getSize();
        long preOffset = lastOffset;
        int i, msgLen;
        for (i = msgSize; i > offset + num; i--) {
            preOffset = getPreOffset(preOffset);
        }
        for (; i > offset; i--) {
            msgLen = getMsgLength(preOffset + 8);
            msgs.add(getMsg(preOffset + 12, msgLen));
            preOffset = getPreOffset(preOffset);
        }
        return msgs;
    }

    private long getPreOffset(long offset) {
        return getActualFile(offset).getLong((int) offset % storeFileSize);
    }

    private byte[] getMsg(long offset, int len) {
        byte[] msg = new byte[len];
        byteBuff2bytes(getActualFile(offset).read((int) offset % storeFileSize, len), 0, msg);
        return msg;
    }

    private int getMsgLength(long offset) {
        return getActualFile(offset).getInt((int) offset % storeFileSize);
    }

    private MappedFile getActualFile(long offset) {
        int fileIndex = (int) offset / storeFileSize;
        MappedFile storeFile = storeFiles.get(fileIndex);
        if (offsetOutOfBound((int) offset % storeFileSize, 8)) {
            storeFile = storeFiles.get(fileIndex + 1);
        }
        return storeFile;
    }

    public static void byteBuff2bytes(ByteBuffer byteBuffer, int offset, byte[] ret) {
        if (offset == -1) offset = byteBuffer.position();
        byteBuffer.get(ret, offset, byteBuffer.limit() - byteBuffer.position());
    }

    private boolean offsetOutOfBound(long offset, int size) {
        return offset < storeFileSize && offset + size > storeFileSize;
    }
}
