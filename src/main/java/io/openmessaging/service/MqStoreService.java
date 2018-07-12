package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.model.QueueStoreData;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MqStoreService {
    private static int storeFileSize = Config.mqStoreFileSize;
    private List<MappedFile> storeFiles = new ArrayList<>(10);
    private static AtomicInteger fileNameIndex = new AtomicInteger(0);
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static long logicOffset = 0;
    private static int indexCount = Config.indexCount;
    private static int msgSize = 64;
    private static Map<String, QueueStoreData> storeDatas = new ConcurrentHashMap<>();
    private AtomicBoolean clearDirectBuff = new AtomicBoolean(false);
//    private IndexService indexService;

    public MqStoreService() {
        Executors.newFixedThreadPool(1, r -> {
            Thread thread = new Thread(r);
            thread.setName("clear-directbuff");
            thread.setDaemon(true);
            return thread;
        }).execute(() -> {
            while (true) {
                if (clearDirectBuff.get()) {
                    storeDatas.forEach((k, v) -> v.clear());
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date()) + "--" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + ": clear directbuff success");
                    break;
                }
            }
        });
    }

    private MappedFile create() {
        return new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize, true);
    }

    public synchronized void put(String queueName, byte[] message) {
        QueueStoreData storeData = storeDatas.get(queueName);
        synchronized (this) {
            if (storeData == null) {
                storeDatas.put(queueName, new QueueStoreData());
                storeData = storeDatas.get(queueName);
            }
            MappedFile writableFile;
            storeData.putDirtyData(message.length).putDirtyData(message).fillDirtyData(msgSize - 4 - message.length);
            int size = storeData.updateSize();
            if (size % indexCount == 0) {
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
                storeData.index(size, logicOffset);
                writableFile.appendData(dirtyData);
                logicOffset += dirtyDataLen;
            }
        }
    }

    public List<byte[]> get(String queueName, long startIndex, int num) {
        if (!clearDirectBuff.get()) clearDirectBuff.compareAndSet(false, true);
        LinkedList<byte[]> msgs = new LinkedList<>();
        QueueStoreData storeData = storeDatas.get(queueName);
        int start = (int) startIndex / indexCount, end = ((int) startIndex + num - 1) / indexCount;
        if (start >= 200) return Collections.emptyList();
        long startPyOffset = storeData.query(start), pyOffset;
        for (int i = (int) startIndex; i < Math.min(storeData.getSize(), (int) startIndex + num); i++) {
            if (start != end && i == end * indexCount) {
                startPyOffset = storeData.query(end);
            }
            pyOffset = startPyOffset + i % indexCount * msgSize;
            msgs.add(getMsg(pyOffset + 4, getMsgLength(pyOffset)));
        }
//        long[] startOffsets = getStartOffset(storeData, startIndex);
//        long startOffset = startOffsets[0];
//        int i = (int) startIndex / indexCount * indexCount, msgLen;
//        for (; i < startIndex; i++) {
//            startOffset += getMsgLength(startOffset) + 4;
//        }
//        int endIndex = Math.min(storeData.getSize(), (int) startIndex + num);
//        if (startOffsets.length == 1) {
//            for (; i < endIndex; i++) {
//                msgLen = getMsgLength(startOffset);
//                msgs.add(getMsg(startOffset + 4, msgLen));
//                startOffset += msgLen + 4;
//            }
//        } else {
//            endIndex = Math.min(endIndex, indexCount * (i / indexCount + 1));
//            for (; i < endIndex; i++) {
//                msgLen = getMsgLength(startOffset);
//                msgs.add(getMsg(startOffset + 4, msgLen));
//                startOffset += msgLen + 4;
//            }
//            endIndex = Math.min(storeData.getSize(), (int) startIndex + num);
//            for (startOffset = startOffsets[1]; i < endIndex; i++) {
//                msgLen = getMsgLength(startOffset);
//                msgs.add(getMsg(startOffset + 4, msgLen));
//                startOffset += msgLen + 4;
//            }
//        }
        return msgs;
    }

    private long[] getStartOffset(QueueStoreData storeData, long startIndex) {
        int index = (int) startIndex / indexCount;
        if (startIndex % indexCount == 0) {
            return new long[]{storeData.query(index)};
        } else {
            return new long[]{storeData.query(index), storeData.query(index + 1)};
        }
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
