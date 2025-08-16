package com.karma.dbbuffer.client.entity;

import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.schema.FieldDefinition;

public interface PEntity<T> {

    T getId();

    String get_$schemaName();

    byte get_$schemaId();

    byte get_$schemaVersion();

    boolean is_$schemaSortingOrder();

    FieldDefinition[] get_$fieldDefinitions();

    void _$clearAllMask();

    default long getVersion() { return Constants.NONE_VERSION; };

    default boolean get_$PrimitiveBoolean(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "boolean"));
    }

    default byte get_$PrimitiveByte(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "byte"));
    }

    default short get_$PrimitiveShort(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "short"));
    }

    default int get_$PrimitiveInt(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "int"));
    }

    default long get_$PrimitiveLong(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "long"));
    }

    default Integer get_$Integer(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "Integer"));
    }

    default Long get_$Long(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "Long"));
    }

    default String get_$String(int index) throws IllegalStateException {
        throw new IllegalStateException(String.format("index %d is not typeof %s", index, "String"));
    }
}
