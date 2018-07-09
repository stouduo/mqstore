package io.openmessaging.service.impl;

import io.openmessaging.config.Config;
import io.openmessaging.model.MappedFile;
import io.openmessaging.service.IndexService;
import io.openmessaging.util.ByteUtil;

public class FileIndexService implements IndexService<Integer> {
    private MappedFile indexFile;
    private static String filePath = Config.rootPath + Config.mqStorePath;
    private static final String INDEX_FILE_NAME = "index.idx";
    private static int INDEX_LENGTH = 8;
    private static int fileSize = queueCount * idxCount * INDEX_LENGTH;

    public FileIndexService() {
        indexFile = new MappedFile(INDEX_FILE_NAME, filePath, fileSize).setFileFlushInterval(5000);
    }

    @Override
    public long query(int queue, Integer key) {
        return indexFile.getLong((queue * idxCount + key) * INDEX_LENGTH);
    }

    @Override
    public void index(int queue, int size, long logicOffset) {
        indexFile.appendData((queue * idxCount + size) * INDEX_LENGTH, ByteUtil.long2Bytes(logicOffset));
    }
}
