package com.karma.dbbuffer.server.compressing.sorting;

import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.schema.FieldDefinition;
import com.karma.dbbuffer.schema.OperationType;
import com.karma.dbbuffer.server.codec.RecordFieldDataDecoder;
import com.karma.dbbuffer.server.compressing.CompressedRecord;
import com.karma.dbbuffer.server.compressing.Compressor;
import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.schema.VersionableSchema;
import com.karma.dbbuffer.server.compressing.Sortable;
import com.karma.dbbuffer.server.recycling.ArrayRecycler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class SortableCompressor<T> implements Compressor<T>, Sortable {

    private static final byte MIN_ALIGN_OFFSET = 2;

    private static final long SAFETY_ALIGN_CYCLES = 5;

    private ArrayRecycler<Record> recordArrayRecycler;

    private ArrayRecycler<Object> objectArrayRecycler;

    private Record[][] values;

    private byte cycleLength;

    private byte alignOffset;

    private long zeroCycle;

    private VersionableSchema lastSchema;

    public SortableCompressor(VersionableSchema schema,
                              ArrayRecycler<Record> recordArrayRecycler, ArrayRecycler<Object> objectArrayRecycler,
                              byte capacity) {
        this.lastSchema = schema;
        this.recordArrayRecycler = recordArrayRecycler;
        this.objectArrayRecycler = objectArrayRecycler;
        this.cycleLength = capacity;
        this.values = new Record[][]{ getRecordArray(capacity) };
        this.zeroCycle = -1;                                                    // means empty values
    }

    @Override
    public void addRecord(Record<T> record) {
        long version = record.getVersion() - alignOffset;
        long cycle = version / cycleLength;
        int index = (int) (version % cycleLength);

        if (zeroCycle < 0) {                                                    // the first element
            if (cycle > SAFETY_ALIGN_CYCLES && index > MIN_ALIGN_OFFSET) {
                alignOffset = (byte) (index - MIN_ALIGN_OFFSET);
                version = record.getVersion() - alignOffset;
                long c = version / cycleLength;
                int i = (int) (version % cycleLength);

                log.debug("array sorter align ZeroCycle({} -> {}), Index({} -> {})", cycle, c, index, i);
                cycle = c;
                index = i;
            }

            zeroCycle = cycle;
            values[0][index] = record;
            log.debug("array sorter init Index({}), Version({})", index, record.getVersion());
        } else {
            int cycleOffset = (int) (cycle - zeroCycle);                        // overflow checked
            if (cycleOffset < 0) {
                int indent = Math.abs(cycleOffset);
                Record[][] vs = new Record[values.length + indent][];
                System.arraycopy(values, 0, vs, indent, values.length);
                for (int i = 0; i < indent; i++) {
                    vs[i] = getRecordArray(cycleLength);
                }
                values = vs;
                zeroCycle -= indent;
                cycleOffset = 0;
                log.debug("array sorter indent {}", indent);
            } else {
                int requiredLength = cycleOffset + 1;
                if (values.length < requiredLength) {
                    log.debug("array sorter expand Length({} -> {})", values.length, requiredLength);
                    Record[][] vs = new Record[requiredLength][];
                    System.arraycopy(values, 0, vs, 0, values.length);
                    for (int i = values.length; i < requiredLength; i++) {
                        vs[i] = getRecordArray(cycleLength);
                    }
                    values = vs;
                }
            }

            Record[] vPool = values[cycleOffset];
            if (vPool[index] != null) {                 // duplicated, the latest is prioritized
                log.debug("array sorter duplicated Version({})", record.getVersion());
            }

            vPool[index] = record;
            log.debug("array sorter add Cycle({}) Index({}), Version({})", cycleOffset, index, record.getVersion());
        }
    }

    @Override
    public void mergeRecords(Sortable sortable) {
        if (sortable == null || sortable.getClass() != getClass()) {
            throw new IllegalArgumentException("sorter must be of " + getClass());
        }

        SortableCompressor sourceSorter = (SortableCompressor) sortable;
        Record[][] sourceValues = sourceSorter.values;

        long lastVersion = Constants.NONE_VERSION;

        for (int i = 0, len = values.length; i < len; i++) {
            Record[] records = values[i];
            for (int j = 0; j < cycleLength; j++) {     // because of applying from recycler, the actual length of records may be greater than cycleLength
                Record record = records[j];
                if (record != null) {
                    lastVersion = record.getVersion();
                } else if (lastVersion > Constants.NONE_VERSION) {
                    long alignedVersion = lastVersion + 1 - sourceSorter.alignOffset;
                    int alignedCycle = (int) (alignedVersion / sourceSorter.cycleLength - sourceSorter.zeroCycle);        // overflow checked
                    int index = (int) (alignedVersion % sourceSorter.cycleLength);

                    if (alignedCycle < sourceValues.length) {
                        if (alignedCycle < 0) {
                            continue;
                        }

                        record = sourceValues[alignedCycle][index];
                        if (record != null) {
                            records[i] = record;
                            lastVersion = record.getVersion();
                            log.debug("array sorter merge Version({})", lastVersion);
                        }
                    } else {
                        return;                 // out of range
                    }
                }
            }
        }

        long alignedVersion = lastVersion + 1 - sourceSorter.alignOffset;
        int alignedCycle = (int) (alignedVersion / sourceSorter.cycleLength - sourceSorter.zeroCycle);        // overflow checked
        int index = (int) (alignedVersion % sourceSorter.cycleLength);

        for (int i = Math.max(0, alignedCycle), len = sourceValues.length; i < len; i++) {
            Record[] records = sourceValues[i];
            for (int j = index; j < sourceSorter.cycleLength; j++) {
                Record record = records[j];
                if (record != null) {
                    log.debug("array sorter merging ...");
                    addRecord(record);
                }
            }
        }
    }

    @Override
    public CompressedRecord<T> compress() throws IOException {
        FieldDefinition[] fieldDefinitions = lastSchema.getFieldDefinitions();
        int totalFields = fieldDefinitions.length;

        byte operation = 0;
        long setMask = 0L, unsetMask = 0L;
        Object[] fieldValues = objectArrayRecycler.get(totalFields);
        long lastVersion = Constants.NONE_VERSION;

        for (int i = values.length - 1; i >= 0; i--) {
            Record[] records = values[i];
            for (int j = cycleLength - 1; j >= 0; j--) {                        // using 'cycleLength' here, the same reason as above
                Record record = records[j];
                if (record == null) {
                    continue;
                }

                if (lastVersion < 0) {
                    lastVersion = record.getVersion();
                }

                byte[] data = record.getData();
                int dataOffset = record.getDataOffset();
                RecordFieldDataDecoder decoder = new RecordFieldDataDecoder(data, dataOffset, data.length - dataOffset);
                byte schemaVersion = decoder.readByte();
                if (schemaVersion != lastSchema.getVersion()) {
                    //TODO deal with a compatible schema
                    log.error("array sorter compress: schema version conflicted {} - {}", schemaVersion, lastSchema.getVersion());
                }

                byte operationSegment = decoder.readByte();
                operation |= (byte) (operationSegment >> 1);

                long setBits = decoder.readFieldMask(lastSchema.getMaxIndex());
                if ((operationSegment & 0x01) == 0x01) {
                    long unsetBits = decoder.readFieldMask(lastSchema.getMaxIndex());
                    unsetBits &= ~setBits;              // remove unset mask which already set by self
                    unsetBits &= ~setMask;              // remove unset mask for all of easier records via the last set mask
                    unsetMask |= unsetBits;             // merge unset mask
                }

                for (int k = 0; k < totalFields; k++) {
                    FieldDefinition fieldDefinition = fieldDefinitions[k];
                    long bitMask = fieldDefinition.getBitMask();
                    if ((bitMask & setBits) != bitMask) {
                        continue;
                    }

                    if ((bitMask & setMask) == bitMask || (bitMask & unsetMask) == bitMask) {
                        decoder.skip(fieldDefinition.getType());
                    } else {
                        setMask |= bitMask;
                        Object value = decoder.readObject(fieldDefinition.getType());
                        fieldValues[k] = value;
                        log.debug("array sorter compress: set record field {}.{} -> {} from Version({})",
                                lastSchema.getName(), fieldDefinition.getFixedName(), value, record.getVersion());
                    }
                }
            }
        }

        if ((operation = OperationType.identify(operation)) == OperationType.UPDATE) {                  // prevent some fields from updating
            long protectMask = getProtectMask();
            setMask &= protectMask;
            unsetMask &= protectMask;
        }
        fillDefaultValue(fieldValues, lastSchema.getFieldDefinitions(), setMask, unsetMask);

        return new CompressedRecord(objectArrayRecycler, operation, lastVersion, setMask | unsetMask, fieldValues);
    }

    @Override
    public void release() {
        for (int i = 0, len = values.length; i < len; i++) {
            recordArrayRecycler.recycle(values[i]);
        }
    }

    private Record[] getRecordArray(byte capacity) {
        return recordArrayRecycler.get(capacity);
    }
}
