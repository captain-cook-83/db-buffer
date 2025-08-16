package com.karma.dbbuffer.server.recycling;

public interface ArrayRecycler<T> {

    T[] get(int capacity);

    void recycle(T[] value);
}
