package io.openmessaging.service.impl;

import io.openmessaging.service.IndexService;

public class HashArrayIndexService implements IndexService<Integer> {
    //    private static int delta = 10;
    private long[][] indices = new long[queueCount][idxCount];


    @Override
    public void index(int queue, int size, long offset) {
        indices[queue][size / indexCount - 1] = offset;
    }

    @Override
    public long query(int queue, Integer key) {
        return indices[queue][key];
    }

}
