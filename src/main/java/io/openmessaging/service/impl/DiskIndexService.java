package io.openmessaging.service.impl;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.service.IndexService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DiskIndexService implements IndexService {
    private MappedFile indexFile;
    private static String filePath = Config.rootPath + Config.indexStorePath;
    private static int indexUnitSize = Config.INDEX_UNIT_SIZE;
    private static int indexFileSize = slotCount * (4 + indexUnitSize * indexUnitCountPerQueue);
    private static Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private static AtomicInteger idGenerator = new AtomicInteger();

    public DiskIndexService() {
        this.indexFile = new MappedFile("index.idx", filePath, indexFileSize);
    }

    @Override
    public long[] get(String key, int index) {
        int pyOffset = pyOffset(key);
        pyOffset += 4 + index * indexUnitSize;
        return new long[]{indexFile.getLong(pyOffset), indexFile.getInt(pyOffset + 8)};
    }

    @Override
    public void put(String key, long offset, int size) {
        try {
            queueIds.putIfAbsent(key, idGenerator.getAndIncrement());
            int pyOffset = pyOffset(key);
            synchronized (this) {
                int endOffset = indexFile.getInt(pyOffset);
                if (endOffset == 0) endOffset += pyOffset + 4;
                indexFile.writeLong(endOffset, offset)
                        .writeInt(endOffset + 8, size)
                        .writeInt(pyOffset, endOffset + indexUnitSize);
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
        int endOffset = Math.min(indexFile.getInt(pyOffset), pyOffset + 4 + (int) (offset + num) * indexUnitSize);
        for (int o = startOffset; o < endOffset; o += indexUnitSize) {
            indices.add(new long[]{indexFile.getLong(o), indexFile.getInt(o + 8)});
        }
        return indices;
    }

    private int pyOffset(String key) {
        return queueIds.get(key) % slotCount * (4 + indexUnitSize * indexUnitCountPerQueue);
    }
}
