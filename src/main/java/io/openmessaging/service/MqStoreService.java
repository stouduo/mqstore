package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.model.QueueMetaData;
import io.openmessaging.service.impl.HashArrayIndexService;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MqStoreService implements IndexService<Integer> {
    private List<MappedFile> storeFiles = new ArrayList<>(10);
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static Map<String, QueueMetaData> metaDatas = new ConcurrentHashMap<>();
    private IndexService indexService;
    private static int indexCount = Config.indexCount;
    private static int blockSize = 128 * 1024;
    private static int blockCountPerFile = 10000;
    private static int totalBlockCount = 1000000;
    private static int fileCount = totalBlockCount / blockCountPerFile + (totalBlockCount % blockCountPerFile == 0 ? 0 : 1);
    private static int fileSize = blockSize * blockCountPerFile;

    public MqStoreService() {
        createFiles();
        indexService = new HashArrayIndexService();
    }

    private void createFiles() {
        for (int i = 0; i < fileCount; i++) {
            storeFiles.add(new MappedFile(MessageFormat.format("mqstore_{0}.data", i), filePath, fileSize));
        }
    }

    public void put(String queueName, byte[] message) {
        metaDatas.putIfAbsent(queueName, new QueueMetaData(blockCountPerFile, blockSize));
        QueueMetaData queueMetaData = metaDatas.get(queueName);
        MappedFile writableFile = storeFiles.get(queueMetaData.getFileIndex());
        synchronized (this) {
            int endOffset = (int) queueMetaData.getEndOffset();
            queueMetaData.setStartOffset(queueMetaData.getEndOffset());
            if (queueMetaData.getMsgCount() % indexCount == 0) {
                indexService.index(queueMetaData);
            }
            writableFile.writeInt(endOffset, message.length);
            writableFile.appendData(endOffset + 4, message);
            queueMetaData.updateMsgCount();
            queueMetaData.updateEndOffset(4 + message.length);
        }
    }

    public List<byte[]> get(String queueName, long readIndex, int num) {
        LinkedList<byte[]> msgs = new LinkedList<>();
        QueueMetaData queueMetaData = metaDatas.get(queueName);
        int endIndex = Math.min((int) readIndex + num, queueMetaData.getMsgCount());
        queueMetaData = getNearestOffset((int) readIndex, queueMetaData);
        MappedFile file = storeFiles.get(queueMetaData.getFileIndex());
        long readOffset = queueMetaData.getStartOffset();
        int i, msgLen;
        for (i = queueMetaData.getMsgCount(); i < readIndex; i++) {
            readOffset += 4 + file.getInt((int) readOffset);
        }
        for (; i < endIndex; i++) {
            msgLen = file.getInt((int) readOffset);
            msgs.add(getMsg(file, readOffset + 4, msgLen));
            readOffset += 4 + msgLen;
        }
        return msgs;
    }


    private QueueMetaData getNearestOffset(int readIndex, QueueMetaData queueMetaData) {
        int index = 0;
        while (index < readIndex) index += indexCount;
        if (index > readIndex) index -= indexCount;
        return indexService.query(queueMetaData, index);
    }

    private byte[] getMsg(MappedFile file, long offset, int len) {
        byte[] msg = new byte[len];
        byteBuff2bytes(file.read((int) offset, len), 0, msg);
        return msg;
    }


    public static void byteBuff2bytes(ByteBuffer byteBuffer, int offset, byte[] ret) {
        if (offset == -1) offset = byteBuffer.position();
        byteBuffer.get(ret, offset, byteBuffer.limit() - byteBuffer.position());
    }

    @Override
    public void index(QueueMetaData flag) {
    }

    @Override
    public QueueMetaData query(QueueMetaData flag, Integer key) {
        return metaDatas.get(flag.getId());
    }
}
