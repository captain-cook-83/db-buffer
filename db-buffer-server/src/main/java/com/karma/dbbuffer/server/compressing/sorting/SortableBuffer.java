package com.karma.dbbuffer.server.compressing.sorting;

import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.server.compressing.BatchRecords;
import com.karma.dbbuffer.server.compressing.Buffer;
import com.karma.dbbuffer.schema.VersionableSchema;
import com.karma.dbbuffer.server.compressing.CompressorFactory;

import java.io.IOException;

public class SortableBuffer<T> implements Buffer<T> {

    private SortableFrame<T> currentFrame;

    private SortableFrame<T> prevFrame;

    public SortableBuffer(VersionableSchema schema, int frameCapacity, CompressorFactory compressorFactory) {
        this.currentFrame = new SortableFrame<>(schema, frameCapacity, compressorFactory);
        this.prevFrame = new SortableFrame<>(schema, frameCapacity, compressorFactory);
    }

    @Override
    public boolean isEmpty() {
        return currentFrame.isEmpty() && prevFrame.isEmpty();
    }

    @Override
    public void addRecord(Record<T> record) throws IOException {
        currentFrame.addRecord(record);
    }

    @Override
    public BatchRecords<T> compress() throws IOException {
        long lastOffset = prevFrame.getLastOffset();
        currentFrame.mergeFrom(prevFrame);
        prevFrame.clear();

        SortableFrame<T> latestFrame = currentFrame;
        currentFrame = prevFrame;
        prevFrame = latestFrame;

        BatchRecords<T> batchRecords = latestFrame.compress();
        batchRecords.setLastOffset(lastOffset);

        return batchRecords;
    }

    @Override
    public void dispose() {
        prevFrame.clear();
        currentFrame.clear();
    }
}
