package com.karma.dbbuffer.server.components;

import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.server.compressing.BatchRecords;
import com.karma.dbbuffer.server.compressing.Buffer;
import com.karma.dbbuffer.server.compressing.CompressorFactory;
import com.karma.dbbuffer.server.compressing.simple.SimpleBuffer;
import com.karma.dbbuffer.server.compressing.sorting.SortableBuffer;
import com.karma.dbbuffer.server.config.ProcessorConfig;
import com.karma.dbbuffer.server.persisting.DatabaseBatchWriter;
import com.karma.dbbuffer.server.serializing.BatchStatement;
import com.karma.dbbuffer.server.serializing.RecordsSerializer;
import com.karma.dbbuffer.schema.VersionableSchema;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class RecordProcessor<T> {

    private ProcessorConfig processorConfig;

    private RecordsSerializer<T> recordsSerializer;

    private DatabaseBatchWriter batchWriter;

    private Map<Byte, Buffer<T>> buffers;

    private long frameEndTime;

    private int frameRecords;

    public RecordProcessor(ProcessorConfig processorConfig,
                           Set<VersionableSchema> schemas, RecordsSerializer<T> recordsSerializer, DatabaseBatchWriter batchWriter,
                           CompressorFactory<T> compressorFactory) {
        this.processorConfig = processorConfig;
        this.recordsSerializer = recordsSerializer;
        this.batchWriter = batchWriter;
        buffers = new HashMap<>(schemas.size());
        for (VersionableSchema schema : schemas) {
            Buffer<T> buffer = schema.isSortingOrder() ?
                    new SortableBuffer<>(schema, processorConfig.getFrameInitRecords(), compressorFactory) :
                    new SimpleBuffer<>(schema, processorConfig.getFrameInitRecords(), compressorFactory);
            buffers.put(schema.getId(), buffer);
        }
    }

    public long addRecord(Record<T> record) throws IOException {
        Buffer<T> buffer = buffers.get(record.getCollection());
        if (buffer == null) {
            log.warn("missing schema for record {}", record);
            return 0;
        }

        buffer.addRecord(record);

        frameRecords += 1;
        if (frameRecords > processorConfig.getFrameCapacity()) {
            return compressAndPersist(System.currentTimeMillis());
        } else {
            return 0;
        }
    }

    public long tickForPersisting() throws IOException {
        long currentTime = System.currentTimeMillis();
        if (currentTime > frameEndTime) {
            if (frameEndTime > 0) {
                return compressAndPersist(currentTime);
            } else {
                frameEndTime = currentTime + processorConfig.getFrameIntervalMs();
            }
        }
        return 0;
    }

    private long compressAndPersist(long currentTime) throws IOException {
        long lastOffset = 0;
        List<BatchRecords<T>> allCollectionRecords = new ArrayList<>(buffers.size());               // most of the time, it will be full filled
        for (Byte collection : buffers.keySet()) {
            Buffer<T> buffer = buffers.get(collection);
            if (buffer.isEmpty()) {
                continue;
            }

            BatchRecords<T> batchRecords = buffer.compress();
            batchRecords.setCollection(collection);
            allCollectionRecords.add(batchRecords);
            lastOffset = Math.max(lastOffset, batchRecords.getLastOffset());
        }

        if (allCollectionRecords.isEmpty()) {
            resetTickTime(currentTime);
            return 0;
        }

        log.debug("batch records Offset({}) Records({})", lastOffset, allCollectionRecords.size());

        List<LinkedList<BatchStatement>> persistStatements =
                allCollectionRecords.stream().map(records -> recordsSerializer.serialize(records)).collect(Collectors.toList());
        batchWriter.write(persistStatements);
        allCollectionRecords.forEach(records -> records.release());             // must be released via the original thread and after write operation
        resetTickTime(currentTime);                                             // reset finally for disperse loading pressure of concurrency

        return lastOffset;
    }

    private void resetTickTime(long currentTime) {
        frameEndTime = currentTime + processorConfig.getFrameIntervalMs();
        frameRecords = 0;
    }

    public void dispose() {
        buffers.forEach((k, v) -> v.dispose());
    }
}
