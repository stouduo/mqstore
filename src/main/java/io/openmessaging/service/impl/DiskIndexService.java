package io.openmessaging.service.impl;

import io.openmessaging.config.Config;
import io.openmessaging.model.Index;
import io.openmessaging.model.MappedFile;
import io.openmessaging.service.IndexService;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DiskIndexService implements IndexService {
    private List<MappedFile> indexFiles = new ArrayList<>();
    private static AtomicInteger fileNameIndex = new AtomicInteger(0);
    private static String filePath = Config.rootPath + Config.indexStorepath;
    private static int indexFileSize = Config.indexFileSize;

    private MappedFile create() {
        return new MappedFile(MessageFormat.format("index_{0}.idx", fileNameIndex.getAndIncrement()), filePath, indexFileSize);
    }

    @Override
    public void put(String key, Index index) {

    }

    @Override
    public List<Index> get(String key) {
        return null;
    }
}
