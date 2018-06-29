package io.openmessaging.service.impl;

import io.openmessaging.config.Config;
import io.openmessaging.model.Index;
import io.openmessaging.model.MappedFile;
import io.openmessaging.service.IndexService;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DiskIndexService implements IndexService {
    private MappedFile indexFile;
    private static String filePath = Config.rootPath + Config.indexStorePath;
    private static int indexUnitSize = Config.INDEX_UNIT_SIZE;
    private static int indexFileSize = slotCount * (4 + indexUnitSize * indexUnitCountPerQueue);
    private static int totalWriteCount = 0;
    private static int lastFlushCount = 0;
    private static MappedByteBuffer mappedByteBuffer;

    public DiskIndexService() {
        this.indexFile = new MappedFile("index.idx", filePath, indexFileSize);
        mappedByteBuffer = indexFile.getMappedByteBuffer();
    }

    @Override
    public Index get(String key, int index) {
        int pyOffset = pyOffset(key);
        pyOffset += 4 + index * indexUnitSize;
        return new Index(mappedByteBuffer.getInt(pyOffset + 8), mappedByteBuffer.getLong(pyOffset));
    }

    @Override
    public synchronized void put(String key, Index index) {
        try {
            int pyOffset = pyOffset(key);
            int endOffset = mappedByteBuffer.getInt(pyOffset);
            if (endOffset == 0) endOffset += pyOffset + 4;
            mappedByteBuffer.putLong(endOffset, index.getOffset());
            mappedByteBuffer.putInt(endOffset + 8, index.getSize());
            mappedByteBuffer.putInt(pyOffset, endOffset + indexUnitSize);
            totalWriteCount++;
            if (totalWriteCount % 1000000 == 0) {
                mappedByteBuffer.force();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Index> get(String key, long offset, long num) {
        List<Index> indices = new ArrayList<>();
        int pyOffset = pyOffset(key);
        int startOffset = pyOffset + 4 + (int) offset * indexUnitSize;
        int endOffset = Math.min(mappedByteBuffer.getInt(pyOffset), pyOffset + 4 + (int) (offset + num) * indexUnitSize);
        for (int o = startOffset; o < endOffset; o += indexFileSize) {
            indices.add(new Index(mappedByteBuffer.getInt(o + 8), mappedByteBuffer.getLong(o)));
        }
        return indices;
    }

    private int pyOffset(String key) {
        return hashKey(key) % slotCount * (4 + indexUnitSize * indexUnitCountPerQueue);
    }
}
