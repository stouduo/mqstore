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
    private static int indexFileSize = slotCount * (indexUnitSize * indexUnitCountPerQueue);
    private static Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private static AtomicInteger idGenerator = new AtomicInteger();
    private static Map<String, Integer> lastIndexOffsets = new ConcurrentHashMap<>();

    public DiskIndexService() {
        this.indexFile = new MappedFile("index.idx", filePath, indexFileSize);
    }

    @Override
    public long[] get(String key, int index) {
        int pyOffset = pyOffset(key);
        pyOffset += index * indexUnitSize;
        return new long[]{indexFile.getLong(pyOffset), indexFile.getInt(pyOffset + 8)};
    }

    @Override
    public void put(String key, long offset, int size) {
        try {
            queueIds.putIfAbsent(key, idGenerator.getAndIncrement());
            int pyOffset = pyOffset(key);
            int endOffset = lastIndexOffsets.getOrDefault(key, pyOffset);
            synchronized (this) {
                indexFile.writeLong(endOffset, offset)
                        .writeInt(endOffset + 8, size);
//                        .writeInt(pyOffset, endOffset + indexUnitSize);
            }
            lastIndexOffsets.put(key, endOffset + indexUnitSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<long[]> get(String key, long offset, long num) {
        List<long[]> indices = new ArrayList<>();
        int pyOffset = pyOffset(key);
        int startOffset = pyOffset + (int) offset * indexUnitSize;
        int endOffset = Math.min(lastIndexOffsets.get(key), pyOffset + (int) (offset + num) * indexUnitSize);
        for (int o = startOffset; o < endOffset; o += indexUnitSize) {
            indices.add(new long[]{indexFile.getLong(o), indexFile.getInt(o + 8)});
        }
        return indices;
    }

    private int pyOffset(String key) {
        return queueIds.get(key) % slotCount * (indexUnitSize * indexUnitCountPerQueue);
    }
}
