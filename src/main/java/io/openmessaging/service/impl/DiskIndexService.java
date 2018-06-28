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
    private static int indexUnitCountPerQueue = Config.indexUnitCountPerQueue;
    private static int slotCount = Config.slotCount;
    private static int indexFileSize = slotCount * (4 + indexUnitSize * indexUnitCountPerQueue);
    private static int totalWriteCount = 0;
    private static int lastFlushCount = 0;

    public DiskIndexService() {
        this.indexFile = new MappedFile("index.idx", filePath, indexFileSize);
    }


    @Override
    public Index get(String key, int index) {
        int slot = hashKey(key) % slotCount;
        int pyOffset = slot * (4 + indexUnitSize * indexUnitCountPerQueue);
        pyOffset += 4 + index * indexUnitSize;
        MappedByteBuffer mappedByteBuffer = indexFile.getMappedByteBuffer();
        return new Index(mappedByteBuffer.getInt(pyOffset + 8), mappedByteBuffer.getLong(pyOffset));
    }

    @Override
    public synchronized void put(String key, Index index) {
        try {
            int slot = hashKey(key) % slotCount;
            int pyOffset = slot * (4 + indexUnitSize * indexUnitCountPerQueue);
            MappedByteBuffer mappedByteBuffer = indexFile.getMappedByteBuffer();
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
    public List<Index> get(String key) {
        List<Index> indices = new ArrayList<>();
        int slot = hashKey(key) % slotCount;
        int pyOffset = slot * (4 + indexUnitSize * indexUnitCountPerQueue);
        MappedByteBuffer mappedByteBuffer = indexFile.getMappedByteBuffer();
        int endOffset = mappedByteBuffer.getInt(pyOffset);
        for (int o = pyOffset + 4; o < endOffset; o += indexFileSize) {
            indices.add(new Index(mappedByteBuffer.getInt(o + 8), mappedByteBuffer.getLong(o)));
        }
        return indices;
    }
}
