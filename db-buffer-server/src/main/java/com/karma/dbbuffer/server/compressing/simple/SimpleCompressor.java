package com.karma.dbbuffer.server.compressing.simple;

import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.server.codec.RecordFieldDataDecoder;
import com.karma.dbbuffer.server.compressing.CompressedRecord;
import com.karma.dbbuffer.server.compressing.Compressor;
import com.karma.dbbuffer.server.recycling.ArrayRecycler;
import com.karma.dbbuffer.schema.FieldDefinition;
import com.karma.dbbuffer.schema.OperationType;
import com.karma.dbbuffer.schema.VersionableSchema;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class SimpleCompressor<T> implements Compressor<T> {

    private ArrayRecycler<Object> objectArrayRecycler;

    private byte operation;

    private long unsetMask;

    private long setMask;

    private Object[] fieldValues;

    private VersionableSchema lastSchema;

    public SimpleCompressor(VersionableSchema schema, ArrayRecycler<Object> objectArrayRecycler) {
        this.lastSchema = schema;
        this.fieldValues = objectArrayRecycler.get(schema.getFieldDefinitions().length);
        this.objectArrayRecycler = objectArrayRecycler;
    }

    @Override
    public void addRecord(Record<T> record) throws IOException {
        FieldDefinition[] fieldDefinitions = lastSchema.getFieldDefinitions();

        byte[] data = record.getData();
        int dataOffset = record.getDataOffset();
        RecordFieldDataDecoder decoder = new RecordFieldDataDecoder(data, dataOffset, data.length - dataOffset);
        byte schemaVersion = decoder.readByte();
        if (schemaVersion != lastSchema.getVersion()) {
            //TODO deal with a compatible schema
            log.error("record schema version conflicted {} - {}", schemaVersion, lastSchema.getVersion());
        }

        byte operationSegment = decoder.readByte();
        operation |= (byte) (operationSegment >> 1);

        long setBits = decoder.readFieldMask(lastSchema.getMaxIndex());
        if ((operationSegment & 0x01) == 0x01) {
            long unsetBits = decoder.readFieldMask(lastSchema.getMaxIndex());
            unsetMask |= unsetBits;             // merge unset mask
            setMask &= ~unsetBits;              // remove set mask for all of easier records
        }
        unsetMask &= ~setBits;              // remove unset mask which already set by self
        setMask |= setBits;                 // merge set mask

        for (int i = 0, len = fieldDefinitions.length; i < len; i++) {
            FieldDefinition fieldDefinition = fieldDefinitions[i];
            long bitMask = fieldDefinition.getBitMask();
            if ((bitMask & setBits) == bitMask) {
                Object value = decoder.readObject(fieldDefinition.getType());
                fieldValues[i] = value;
                log.debug("update record field {}.{} -> {}", lastSchema.getName(), fieldDefinition.getFixedName(), value);
            }
        }

        log.debug("Final Result {} -> Operation({}), SetBits({}), UnsetBits({}), FieldValues({})",
                lastSchema.getName(), operation, setBits, unsetMask, fieldValues);
    }

    @Override
    public CompressedRecord<T> compress() {
        Object[] vs = fieldValues;
        fieldValues = null;

        if ((operation = OperationType.identify(operation)) == OperationType.UPDATE) {                      // prevent some fields from updating
            long protectMask = getProtectMask();
            setMask &= protectMask;
            unsetMask &= protectMask;
        }
        fillDefaultValue(vs, lastSchema.getFieldDefinitions(), setMask, unsetMask);

        return new CompressedRecord<T>(objectArrayRecycler, operation, Constants.NONE_VERSION, setMask | unsetMask, vs);
    }

    @Override
    public void release() {
        if (fieldValues != null) {
            objectArrayRecycler.recycle(fieldValues);
        }
    }
}
