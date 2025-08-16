package com.karma.dbbuffer.server.compressing.simple;

import com.karma.dbbuffer.schema.OperationType;
import com.karma.dbbuffer.schema.VersionableSchema;
import com.karma.dbbuffer.server.compressing.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

public class SimpleFrame<T> extends Frame<T> {

    public SimpleFrame(VersionableSchema schema, CompressorFactory<T> compressorFactory, int capacity) {
        super(schema, compressorFactory, capacity);
    }

    @Override
    public BatchRecords compress() throws IOException {
        LinkedList<CompressedRecord<T>> newRecords = new LinkedList<>();
        LinkedList<CompressedRecord<T>> changedRecord = new LinkedList<>();
        LinkedList<T> deletedIds = new LinkedList<>();
        for (Map.Entry<T, Compressor<T>> entry : idCompressors.entrySet()) {
            Compressor<T> compressor = entry.getValue();
            CompressedRecord<T> record = compressor.compress();
            compressor.release();
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
        idCompressors.clear();                          // TODO dynamic recycle
        return new BatchRecords<T>(newRecords, changedRecord, deletedIds);
    }
}
