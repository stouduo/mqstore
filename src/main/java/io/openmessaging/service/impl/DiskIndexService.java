package io.openmessaging.service.impl;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.service.IndexService;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DiskIndexService implements IndexService {
    private MappedFile indexFile;
    private static String filePath = Config.rootPath + Config.indexStorePath;
    private static int indexUnitSize = Config.INDEX_UNIT_SIZE;
    private static int indexFileSize = slotCount * (4 + indexUnitSize * indexUnitCountPerQueue);
    private static AtomicInteger totalWriteCount = new AtomicInteger(0);
    private static int lastFlushCount = 0;
    private static MappedByteBuffer mappedByteBuffer;

    public DiskIndexService() {
        this.indexFile = new MappedFile("index.idx", filePath, indexFileSize);
        mappedByteBuffer = indexFile.getMappedByteBuffer();
    }

    @Override
    public long[] get(String key, int index) {
        int pyOffset = pyOffset(key);
        pyOffset += 4 + index * indexUnitSize;
        return new long[]{mappedByteBuffer.getLong(pyOffset), mappedByteBuffer.getInt(pyOffset + 8)};
    }

    @Override
    public synchronized void put(String key, long offset, int size) {
        try {
            int pyOffset = pyOffset(key);
            int endOffset = mappedByteBuffer.getInt(pyOffset);
            if (endOffset == 0) endOffset += pyOffset + 4;
            mappedByteBuffer.putLong(endOffset, offset);
            mappedByteBuffer.putInt(endOffset + 8, size);
            mappedByteBuffer.putInt(pyOffset, endOffset + indexUnitSize);
            totalWriteCount.getAndIncrement();
            if (totalWriteCount.get() % 10000000 == 0) {
//                System.out.println("flush index to disk:" + totalWriteCount);
                mappedByteBuffer.force();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<long[]> get(String key, long offset, long num) {
        List<long[]> indices = new ArrayList<>();
        int pyOffset = pyOffset(key);
        int startOffset = pyOffset + 4 + (int) offset * indexUnitSize;
        int endOffset = Math.min(mappedByteBuffer.getInt(pyOffset), pyOffset + 4 + (int) (offset + num) * indexUnitSize);
        for (int o = startOffset; o < endOffset; o += indexFileSize) {
            indices.add(new long[]{mappedByteBuffer.getLong(o), mappedByteBuffer.getInt(o + 8)});
        }
        return indices;
    }

    private int pyOffset(String key) {
        return hashKey(key) % slotCount * (4 + indexUnitSize * indexUnitCountPerQueue);
    }
}
