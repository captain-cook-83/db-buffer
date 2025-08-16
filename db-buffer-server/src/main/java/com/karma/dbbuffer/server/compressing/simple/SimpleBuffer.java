package com.karma.dbbuffer.server.compressing.simple;

import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.server.compressing.BatchRecords;
import com.karma.dbbuffer.server.compressing.Buffer;
import com.karma.dbbuffer.server.compressing.CompressorFactory;
import com.karma.dbbuffer.server.compressing.Frame;
import com.karma.dbbuffer.server.recycling.ArrayRecycler;
import com.karma.dbbuffer.schema.VersionableSchema;

import java.io.IOException;

public class SimpleBuffer<T> implements Buffer<T> {

    private Frame<T> frame;

    public SimpleBuffer(VersionableSchema schema, int frameCapacity, CompressorFactory<T> compressorFactory) {
        this.frame = new SimpleFrame<>(schema, compressorFactory, frameCapacity);
    }

    @Override
    public boolean isEmpty() {
        return frame.isEmpty();
    }

    @Override
    public void addRecord(Record<T> record) throws IOException {
        frame.addRecord(record);
    }

    @Override
    public BatchRecords<T> compress() throws IOException {
        BatchRecords<T> batchRecords = frame.compress();
        batchRecords.setLastOffset(frame.getLastOffset());
        return batchRecords;
    }

    @Override
    public void dispose() {
        frame.clear();
    }
}
