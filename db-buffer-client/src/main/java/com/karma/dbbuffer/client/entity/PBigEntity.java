package com.karma.dbbuffer.client.entity;

public interface PBigEntity<T> extends PEntity<T> {

    long get_$insertMask();

    long get_$setMask();

    long get_$unsetMask();
}
