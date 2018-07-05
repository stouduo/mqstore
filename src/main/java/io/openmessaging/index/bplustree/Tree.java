package io.openmessaging.index.bplustree;

public interface Tree<T> {
    public Object get(Comparable key);   //查询

    public void remove(Comparable key);    //移除

    public void insertOrUpdate(Comparable key, T obj); //插入或者更新，如果已经存在，就更新，否则插入
} 