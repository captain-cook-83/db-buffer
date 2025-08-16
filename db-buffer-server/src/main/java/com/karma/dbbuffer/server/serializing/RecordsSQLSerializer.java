package com.karma.dbbuffer.server.serializing;

import com.karma.commons.utils.ListUtils;
import com.karma.dbbuffer.schema.FieldDefinition;
import com.karma.dbbuffer.server.compressing.BatchRecords;
import com.karma.dbbuffer.schema.SchemaRegistry;
import com.karma.dbbuffer.schema.VersionableSchema;
import com.karma.dbbuffer.server.compressing.CompressedRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RecordsSQLSerializer implements RecordsSerializer<Long> {

    private final int batchInsertSize;

    private final Map<Byte, String> insertMap;

    private final Map<Byte, Map<Long, String>> updateMap;

    private final Map<Byte, String> deleteMap;

    private long warmupTime;

    public RecordsSQLSerializer(int batchInsertSize, int totalCollections, int warmupDuration) {
        this.batchInsertSize = batchInsertSize;
        this.insertMap = new ConcurrentHashMap<>(totalCollections);
        this.updateMap = new ConcurrentHashMap<>(totalCollections);
        this.deleteMap = new ConcurrentHashMap<>(totalCollections);
        this.warmupTime = System.currentTimeMillis() + warmupDuration;
    }

    public LinkedList<BatchStatement> serialize(BatchRecords<Long> records) {
        VersionableSchema schema = SchemaRegistry.getInstance().getSchema(records.getCollection());
        LinkedList<BatchStatement> batchSQLList = new LinkedList<>();

        // insert first
        String insertSQL = insertMap.computeIfAbsent(schema.getId(), id -> generateInsertSQL(schema));
        if (warmupTime > System.currentTimeMillis()) {
            insertSQL += " ON DUPLICATE KEY UPDATE";        // prevent duplicate key from prev startup, it happens for sortable record because of lazy commit for kafka offset.
        }

        LinkedList<CompressedRecord<Long>> insertRecords = records.getNewRecords();
        int insertSize = insertRecords.size();
        if (insertSize > 0) {
            List<Object[]> batchValues = new ArrayList<>(insertSize);
            Iterator<CompressedRecord<Long>> recordIterator = insertRecords.iterator();
            while (recordIterator.hasNext()) {
                CompressedRecord<Long> record = recordIterator.next();
                batchValues.add(record.getValues());
            }
            batchSQLList.add(new BatchStatement(schema, insertSQL, batchValues));
        }

        // updates
        if (records.getChangedRecords().size() > 0) {
            Map<Long, LinkedList<CompressedRecord<Long>>> bitsRecordMap = new HashMap<>();
            Iterator<CompressedRecord<Long>> recordIterator = records.getChangedRecords().iterator();
            while (recordIterator.hasNext()) {
                CompressedRecord<Long> record = recordIterator.next();
                LinkedList<CompressedRecord<Long>> recordList = bitsRecordMap.computeIfAbsent(record.getBitsMask(), mask -> new LinkedList<>());
                recordList.add(record);
            }

            Map<Long, String> schemaUpdateMap = updateMap.computeIfAbsent(schema.getId(), id -> new ConcurrentHashMap<>(8));
            for (Map.Entry<Long, LinkedList<CompressedRecord<Long>>> entry : bitsRecordMap.entrySet()) {
                String updateSQL = schemaUpdateMap.computeIfAbsent(entry.getKey(), id -> generateUpdateSQL(schema, entry.getKey()));
                LinkedList<CompressedRecord<Long>> updateRecords = records.getNewRecords();
                List<Object[]> batchValues = new ArrayList<>(updateRecords.size());
                recordIterator = records.getChangedRecords().iterator();
                while (recordIterator.hasNext()) {
                    batchValues.add(recordIterator.next().getValues());
                }
                batchSQLList.add(new BatchStatement(schema, updateSQL, batchValues));
            }
        }

        // delete last
        if (records.getDeletedIds().size() > 0) {
            StringBuilder sqlBuffer = new StringBuilder();
            sqlBuffer.append(deleteMap.computeIfAbsent(schema.getId(), id -> "DELETE FROM `" + schema.getName() + "` WHERE `id` IN ("));
            ListUtils.join(records.getDeletedIds(), ",", sqlBuffer);
            sqlBuffer.append(')');
            batchSQLList.add(new BatchStatement(schema, sqlBuffer.toString(), null));
        }

        return batchSQLList;
    }

    private static String generateInsertSQL(VersionableSchema schema) {
        FieldDefinition[] fieldDefinitions = schema.getFieldDefinitions();
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = fieldDefinitions.length; i < len; i++) {
            builder.append(",`");
            builder.append(fieldDefinitions[i].getFixedName());
            builder.append('`');
        }
        String fieldNames = builder.substring(1);

        builder.setLength(0);
        for (int i = 0, len = fieldDefinitions.length; i < len; i++) {
            builder.append(",?");
        }
        String fieldValues = builder.substring(1);

        return "INSERT INTO `" + schema.getName() + "` (" + fieldNames + ") VALUES (" + fieldValues + ")";
    }

    private static String generateUpdateSQL(VersionableSchema schema, long bitMask) {
        FieldDefinition[] fieldDefinitions = schema.getFieldDefinitions();
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = fieldDefinitions.length; i < len; i++) {
            FieldDefinition fieldDefinition = fieldDefinitions[i];
            if ((fieldDefinition.getBitMask() & bitMask) == fieldDefinition.getBitMask()) {
                builder.append(",`");
                builder.append(fieldDefinition.getFixedName());
                builder.append("`=?");
            }
        }

        String setSegments = builder.substring(1);
        return "UPDATE `" + schema.getName() + "` SET " + setSegments + " WHERE `id`=?";
    }
}