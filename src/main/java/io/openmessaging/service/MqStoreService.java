package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.util.ByteUtil;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MqStoreService {
    private static int storeFileSize = Config.mqStoreFileSize;
    private List<MappedFile> storeFiles = new ArrayList<>(10);
    private static AtomicInteger fileNameIndex = new AtomicInteger(0);
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static long logicOffset = 0;
    private static final int MAGIC_CODE = 0x1dcfc;

    public MqStoreService() {
        init();
    }

    private void init() {
        storeFiles.add(create());
    }

    private MappedFile create() {
        MappedFile file = new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize);
        file.boundChannelToByteBuffer();
        try {
            file.appendData(ByteUtil.int2Bytes(MAGIC_CODE));
            this.logicOffset += 4;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return file;
    }

    public synchronized long put(byte[] message) {
        MappedFile writableFile = storeFiles.get(storeFiles.size() - 1);
        long retOffset = logicOffset, fileOffset = logicOffset % storeFileSize;
        try {
            if (offsetOutOfBound(fileOffset, message.length)) {
                writableFile.appendData(message, 0, (int) (storeFileSize - fileOffset));
                writableFile = create();
                writableFile.appendData(message, (int) (storeFileSize - fileOffset + 1), (int) (fileOffset + message.length - storeFileSize));
                storeFiles.add(writableFile);
            } else {
                writableFile.appendData(message);
            }
            logicOffset += message.length;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retOffset;
    }

    public byte[] get(long offset, int size) {
        byte[] ret = new byte[size];
        int fileIndex = (int) offset / storeFileSize;
        int fileOffset = (int) offset % storeFileSize;
        if (offsetOutOfBound(fileOffset, size)) {
            byteBuff2bytes(storeFiles.get(fileIndex).read(fileOffset, storeFileSize - fileOffset), ret);
            byteBuff2bytes(storeFiles.get(fileIndex + 1).read(4, fileOffset + size - storeFileSize), ret);
        } else byteBuff2bytes(storeFiles.get(fileIndex).read(fileOffset, size), ret);
        return ret;
    }

    public static void byteBuff2bytes(ByteBuffer byteBuffer, byte[] ret) {
        byteBuffer.get(ret, byteBuffer.position(), byteBuffer.limit() - byteBuffer.position());
    }

    private boolean offsetOutOfBound(long offset, int size) {
        return offset < storeFileSize && offset + size > storeFileSize;
    }
}
