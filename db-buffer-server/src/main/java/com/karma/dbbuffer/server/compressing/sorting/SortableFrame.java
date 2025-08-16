package com.karma.dbbuffer.server.compressing.sorting;

import com.karma.dbbuffer.schema.OperationType;
import com.karma.dbbuffer.server.compressing.*;
import com.karma.dbbuffer.schema.VersionableSchema;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

public class SortableFrame<T> extends Frame<T> {

    public SortableFrame(VersionableSchema schema, int capacity, CompressorFactory<T> compressorFactory) {
        super(schema, compressorFactory, capacity);
    }

    @Override
    public BatchRecords compress() throws IOException {
        LinkedList<CompressedRecord<T>> newRecords = new LinkedList<>();
        LinkedList<CompressedRecord<T>> changedRecord = new LinkedList<>();
        LinkedList<T> deletedIds = new LinkedList<>();
        for (Map.Entry<T, Compressor<T>> entry : idCompressors.entrySet()) {
            CompressedRecord<T> record = entry.getValue().compress();
            record.setId(entry.getKey());
            switch (record.getOperation()) {
                case OperationType.INSERT:
                    newRecords.add(record);
                    break;
                case OperationType.UPDATE:
                    changedRecord.add(record);
                    break;
                case OperationType.DELETE:
                    deletedIds.add(record.getId());
                    record.release();
                    break;
            }
        }
        return new BatchRecords<T>(newRecords, changedRecord, deletedIds);
    }

    public void mergeFrom(SortableFrame<T> sourceFrame) {
        Map<T, Compressor<T>> sourceCompressors = sourceFrame.idCompressors;
        if (sourceCompressors.isEmpty()) {
            return;
        }

        for (Map.Entry<T, Compressor<T>> entry : idCompressors.entrySet()) {
            Compressor<T> compressor = entry.getValue();
            if (compressor instanceof Sortable) {
                Compressor<T> sourceCompressor = sourceCompressors.get(entry.getKey());
                if (sourceCompressor != null && sourceCompressor.getClass() == compressor.getClass()) {
                    ((Sortable) compressor).mergeRecords((Sortable) sourceCompressor);
                }
            }
        }
    }
}
