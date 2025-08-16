package com.karma.dbbuffer.server.compressing;

import com.karma.dbbuffer.schema.VersionableSchema;
import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.schema.OperationType;

import java.io.IOException;
import java.util.*;

public abstract class Frame<T> {

    private VersionableSchema schema;

    private CompressorFactory<T> compressorFactory;

    protected Map<T, Compressor<T>> idCompressors;

    private long lastOffset;

    public Frame(VersionableSchema schema, CompressorFactory<T> compressorFactory, int capacity) {
        this.schema = schema;
        this.compressorFactory = compressorFactory;
        this.idCompressors = new HashMap<>(capacity);
    }

    public boolean isEmpty() { return idCompressors.isEmpty(); }

    public long getLastOffset() {
        return lastOffset;
    }

    public void addRecord(Record<T> record) throws IOException {
        idCompressors.computeIfAbsent(record.getId(), id -> compressorFactory.createCompressor(schema)).addRecord(record);
        lastOffset = record.getOffset();
    }

    public void clear() {
        idCompressors.forEach((id, compressor) -> compressor.release());
        idCompressors.clear();
    }

    public abstract BatchRecords compress() throws IOException;
}
