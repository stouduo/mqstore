package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.model.QueueStoreData;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MqStoreService {
    private List<MappedFile> storeFiles = new ArrayList<>(10);
    private static AtomicInteger fileNameIndex = new AtomicInteger(0);
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static int queueMsgCountPerFile = Config.queueMsgCountPerFile;
    private static int msgSize = 64;
    private static int storeFileSize = 1000000 * queueMsgCountPerFile * msgSize;
    private static Map<String, QueueStoreData> storeDatas = new ConcurrentHashMap<>();

    private MappedFile create() {
        return new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize, true);
    }

    public void put(String queueName, byte[] message) {
        QueueStoreData storeData = storeDatas.get(queueName);
        if (storeData == null) {
            synchronized (this) {
                if (storeData == null) {
                    storeData = new QueueStoreData();
                    storeDatas.put(queueName, storeData);
                }
            }
        }
        int msgIndex, fileIndex, msgPyOffset;
        synchronized (this) {
            msgIndex = storeData.updateMsgIndex();
            fileIndex = msgIndex / queueMsgCountPerFile;
            if (storeFiles.size() <= fileIndex) {
                storeFiles.add(create());
            }
            msgPyOffset = (storeData.getId() * queueMsgCountPerFile + msgIndex) * msgSize % storeFileSize;
        }
        MappedFile writableFile = storeFiles.get(fileIndex);
        byte[] fill = new byte[msgSize - 4 - message.length];
        writableFile.writeInt(msgPyOffset, message.length)
                .appendData(msgPyOffset + 4, message)
                .appendData(msgPyOffset + 4 + message.length, fill);
    }

    public List<byte[]> get(String queueName, long startIndex, int num) {
        LinkedList<byte[]> msgs = new LinkedList<>();
        QueueStoreData storeData = storeDatas.get(queueName);
        int start = (int) startIndex, end = Math.min(start + num, storeData.getMsgIndex());
        int fileIndex0 = start / queueMsgCountPerFile, fileIndex1 = (end - 1) / queueMsgCountPerFile;
        int startOffset = storeData.getId() * queueMsgCountPerFile * msgSize, realOffset;
        if (fileIndex0 >= storeFiles.size()) return Collections.emptyList();
        MappedFile file = storeFiles.get(fileIndex0);
        for (int i = start; i < end; i++) {
            if (fileIndex0 != fileIndex1 && i == fileIndex1 * queueMsgCountPerFile) {
                file = storeFiles.get(fileIndex1);
                startOffset = storeData.getId() * queueMsgCountPerFile * msgSize;
            }
            realOffset = (startOffset + i * msgSize) % storeFileSize;
            msgs.add(getMsg(file, realOffset + 4, file.readByChannel(realOffset)));
        }
        return msgs;
    }

    private byte[] getMsg(MappedFile file, long offset, int len) {
        byte[] msg = new byte[len];
        byteBuff2bytes(file.readByChannel((int) (offset), len), 0, msg);
        return msg;
    }

    public static void byteBuff2bytes(ByteBuffer byteBuffer, int offset, byte[] ret) {
        if (offset == -1) offset = byteBuffer.position();
        byteBuffer.get(ret, offset, byteBuffer.limit() - byteBuffer.position());
    }

}
