package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.model.QueueStoreData;
import io.openmessaging.service.impl.FileIndexService;
import io.openmessaging.service.impl.HashArrayIndexService;
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
    private static int indexCount = Config.indexCount;
    private static Map<String, QueueStoreData> storeDatas = new ConcurrentHashMap<>();
    private IndexService indexService;

    public MqStoreService() {
//        indexService = new HashArrayIndexService();
        indexService = new FileIndexService();
    }

    private MappedFile create() {
        return new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize).setFileFlushSize(50 * 1024 * 1024);
    }

    public void put(String queueName, byte[] message) {
        storeDatas.putIfAbsent(queueName, new QueueStoreData());
        MappedFile writableFile;
        QueueStoreData storeData = storeDatas.get(queueName);
        synchronized (queueName.intern()) {
            storeData.putDirtyData(message.length).putDirtyData(message);
            storeData.updateSize();
        }
        synchronized (this) {
            if (storeData.getSize() % indexCount == 0) {
                long fileOffset = logicOffset % storeFileSize;
                ByteBuffer dirtyData = storeData.getDirtyData();
                int dirtyDataLen = dirtyData.position();
                if (fileOffset == 0) {
                    storeFiles.add(create());
                }
                writableFile = storeFiles.get(storeFiles.size() - 1);
                if (offsetOutOfBound(fileOffset, dirtyDataLen)) {
                    writableFile = create();
                    storeFiles.add(writableFile);
                    logicOffset += storeFileSize - fileOffset;
                }
                indexService.index(storeData.getId(), storeData.getSize(), logicOffset);
                writableFile.appendData(dirtyData);
                logicOffset += dirtyDataLen;
            }
        }
    }

    public List<byte[]> get(String queueName, long startIndex, int num) {
        LinkedList<byte[]> msgs = new LinkedList<>();
        QueueStoreData storeData = storeDatas.get(queueName);
        long[] startOffsets = getStartOffset(storeData.getId(), startIndex);
        long startOffset = startOffsets[0];
        int i = (int) startIndex / indexCount * indexCount, msgLen;
        for (; i < startIndex; i++) {
            startOffset += getMsgLength(startOffset) + 4;
        }
        int endIndex = Math.min(storeData.getSize(), (int) startIndex + num);
        if (startOffsets.length == 1) {
            for (; i < endIndex; i++) {
                msgLen = getMsgLength(startOffset);
                msgs.add(getMsg(startOffset + 4, msgLen));
                startOffset += msgLen + 4;
            }
        } else {
            endIndex = Math.min(endIndex, indexCount * (i / indexCount + 1));
            for (; i < endIndex; i++) {
                msgLen = getMsgLength(startOffset);
                msgs.add(getMsg(startOffset + 4, msgLen));
                startOffset += msgLen + 4;
            }
            endIndex = Math.min(storeData.getSize(), (int) startIndex + num);
            for (startOffset = startOffsets[1]; i < endIndex; i++) {
                msgLen = getMsgLength(startOffset);
                msgs.add(getMsg(startOffset + 4, msgLen));
                startOffset += msgLen + 4;
            }
        }
        return msgs;
    }

    private long[] getStartOffset(int queue, long startIndex) {
        int index = (int) startIndex / indexCount;
        if (startIndex % indexCount == 0) {
            return new long[]{indexService.query(queue, index)};
        } else
            return new long[]{indexService.query(queue, index), indexService.query(queue, index + 1)};
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

}
