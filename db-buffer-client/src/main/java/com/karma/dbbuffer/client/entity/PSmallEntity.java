package com.karma.dbbuffer.client.entity;

public interface PSmallEntity<T> extends PEntity<T> {

    int get_$insertMask();

    int get_$setMask();

    int get_$unsetMask();
}
