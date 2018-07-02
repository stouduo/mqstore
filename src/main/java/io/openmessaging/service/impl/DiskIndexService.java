package io.openmessaging.service.impl;

import io.openmessaging.config.Config;
import io.openmessaging.model.Index;
import io.openmessaging.model.MappedFile;
import io.openmessaging.service.IndexService;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DiskIndexService implements IndexService {
    private List<MappedFile> indexFiles = new ArrayList<>();
    private List<MappedByteBuffer> mappedByteBuffers = new ArrayList<>();
    private static int indexFileCount = Config.indexFileCount;
    private static String filePath = Config.rootPath + Config.indexStorePath;
    private static int indexUnitSize = Config.INDEX_UNIT_SIZE;
    private static int slotCountPerIndexFile = slotCount / indexFileCount;
    private static int indexFileSize = slotCountPerIndexFile * (indexUnitSize * indexUnitCountPerQueue);
    private static AtomicInteger totalWriteCount = new AtomicInteger(0);
    private static Map<String, Integer> queueIds = new ConcurrentHashMap<>();
    private static AtomicInteger idGene = new AtomicInteger(0);
    private static Map<String, Integer> lastIndexOffsets = new ConcurrentHashMap<>();

    public DiskIndexService() {
        for (int i = 0; i < indexFileCount; i++) {
            indexFiles.add(new MappedFile("index_" + i + ".idx", filePath, indexFileSize));
            mappedByteBuffers.add(indexFiles.get(i).getMappedByteBuffer());
        }
    }

    @Override
    public long[] get(String key, int index) {
        int pyOffset = pyOffset(key);
        MappedByteBuffer mappedByteBuffer = mappedByteBuffers.get(fileIndex(key));
        pyOffset += index * indexUnitSize;
        return new long[]{mappedByteBuffer.getLong(pyOffset), mappedByteBuffer.getInt(pyOffset + 8)};
    }

    @Override
    public void put(String key, long offset, int size) {
        try {
            queueIds.putIfAbsent(key, idGene.getAndIncrement());
            int pyOffset = pyOffset(key);
            MappedByteBuffer mappedByteBuffer = mappedByteBuffers.get(fileIndex(key));
            int endOffset = lastIndexOffsets.getOrDefault(key, pyOffset);
//            synchronized (this) {
//                int endOffset = mappedByteBuffer.getInt(pyOffset);
//                if (endOffset == 0) endOffset += pyOffset + 4;
            mappedByteBuffer.putLong(endOffset, offset);
            mappedByteBuffer.putInt(endOffset + 8, size);
            lastIndexOffsets.put(key, endOffset + indexUnitSize);
//                mappedByteBuffer.putInt(pyOffset, endOffset + indexUnitSize);
//            }
            totalWriteCount.getAndIncrement();
            if (totalWriteCount.get() % 5000000 == 0) {
//                System.out.println("flush index to disk:" + totalWriteCount);
                mappedByteBuffers.forEach(buffer -> buffer.force());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<long[]> get(String key, long offset, long num) {
        List<long[]> indices = new ArrayList<>();
        int pyOffset = pyOffset(key);
        MappedByteBuffer mappedByteBuffer = mappedByteBuffers.get(fileIndex(key));
        int startOffset = pyOffset + (int) offset * indexUnitSize;
        int endOffset = Math.min(lastIndexOffsets.get(key), pyOffset + (int) (offset + num) * indexUnitSize);
        for (int o = startOffset; o < endOffset; o += indexUnitSize) {
            indices.add(new long[]{mappedByteBuffer.getLong(o), mappedByteBuffer.getInt(o + 8)});
        }
        return indices;
    }

    private int pyOffset(String key) {
        return queueIds.get(key) / indexFileCount * (indexUnitSize * indexUnitCountPerQueue);
    }

    private int fileIndex(String key) {
        return queueIds.get(key) % indexFileCount;
    }
}
