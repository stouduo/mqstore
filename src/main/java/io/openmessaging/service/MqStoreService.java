package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.model.QueueStoreData;
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
        indexService = new HashArrayIndexService();
    }

    private MappedFile create() {
        return new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize).setFileFlushSize(50 * 1024 * 1024);
    }

    public void put(String queueName, byte[] message) {
        storeDatas.putIfAbsent(queueName, new QueueStoreData());
        MappedFile writableFile;
        QueueStoreData storeData = storeDatas.get(queueName);
        byte[] data = new byte[4 + message.length];
        ByteUtil.byteMerger(data, ByteUtil.int2Bytes(message.length), message);
        synchronized (this) {
            storeData.setDirtyData(data);
            storeData.updateSize();
            if (storeData.getSize() % indexCount == 0) {
                indexService.index(storeData.getId(), storeData.getSize(), logicOffset);
                long fileOffset = logicOffset % storeFileSize;
                if (fileOffset == 0) {
                    storeFiles.add(create());
                }
                writableFile = storeFiles.get(storeFiles.size() - 1);
                if (offsetOutOfBound(fileOffset, 4 + message.length)) {
                    writableFile = create();
                    storeFiles.add(writableFile);
                    logicOffset += storeFileSize - fileOffset;
                }
                writableFile.appendData(storeData.getDirtyData());
                logicOffset += storeData.getDirtyData().capacity();
            }
        }
    }

    public List<byte[]> get(String queueName, long startIndex, int num) {
        LinkedList<byte[]> msgs = new LinkedList<>();
        QueueStoreData storeData = storeDatas.get(queueName);
        long[] startOffsets = getStartOffset(storeData.getId(), startIndex);
        long startOffset = startOffsets[0];
        int i = 0, msgLen;
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
            for (; i < Math.min(endIndex, indexCount - i); i++) {
                msgLen = getMsgLength(startOffset);
                msgs.add(getMsg(startOffset + 4, msgLen));
                startOffset += msgLen + 4;
            }
            for (startOffset = startOffsets[1]; i < endIndex; i++) {
                msgLen = getMsgLength(startOffset);
                msgs.add(getMsg(startOffset + 4, msgLen));
                startOffset += msgLen + 4;
            }
        }
        return msgs;
    }

    private long[] getStartOffset(int queue, long startIndex) {
        int index = 0;
        for (; index < startIndex; index += indexCount) ;
        if (startIndex % indexCount == 0) {
            if (startIndex != 0) index -= indexCount;
            return new long[]{indexService.query(queue, index)};
        } else
            return new long[]{indexService.query(queue, index - indexCount), indexService.query(queue, index)};
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
