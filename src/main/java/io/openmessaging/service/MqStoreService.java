package io.openmessaging.service;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.util.ByteUtil;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MqStoreService {
    private static int storeFileSize = Config.mqStoreFileSize;
    private List<MappedFile> storeFiles = new ArrayList<>(10);
    private static AtomicInteger fileNameIndex = new AtomicInteger(0);
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static long logicOffset = 0;
    private static final int MAGIC_CODE = 0x1dcfc;

    public MqStoreService() {
//        init();
    }

//    private void init() {
//        storeFiles.add(create());
//    }

    private MappedFile create() {
        MappedFile file = new MappedFile(MessageFormat.format("mqstore_{0}.data", fileNameIndex.getAndIncrement()), filePath, storeFileSize);
        file.boundChannelToByteBuffer();
        try {
            file.appendData(ByteUtil.int2Bytes(MAGIC_CODE));
            logicOffset += 4;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return file;
    }

    public synchronized long put(byte[] message) {
        long retOffset = 0, fileOffset = logicOffset % storeFileSize;
        try {
            MappedFile writableFile;
            if (fileOffset == 0) {
                storeFiles.add(create());
            }
            fileOffset = logicOffset % storeFileSize;
            retOffset = logicOffset;
            writableFile = storeFiles.get(storeFiles.size() - 1);
            if (offsetOutOfBound(fileOffset, message.length)) {
                int offset = (int) (storeFileSize - fileOffset);
                writableFile.appendData(message, 0, offset);
                writableFile = create();
                writableFile.appendData(message, offset, (int) (fileOffset + message.length - storeFileSize));
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
            byteBuff2bytes(storeFiles.get(fileIndex).read(fileOffset, storeFileSize - fileOffset), 0, ret);
            byteBuff2bytes(storeFiles.get(fileIndex + 1).read(4, fileOffset + size - storeFileSize), 3, ret);
        } else byteBuff2bytes(storeFiles.get(fileIndex).read(fileOffset, size), -1, ret);
        return ret;
    }

    public static void byteBuff2bytes(ByteBuffer byteBuffer, int offset, byte[] ret) {
        if (offset == -1) offset = byteBuffer.position();
        byteBuffer.get(ret, offset, byteBuffer.limit() - byteBuffer.position());
    }

    private boolean offsetOutOfBound(long offset, int size) {
        return offset < storeFileSize && offset + size > storeFileSize;
    }
}
