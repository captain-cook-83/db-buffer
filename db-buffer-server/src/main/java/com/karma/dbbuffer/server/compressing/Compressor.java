package com.karma.dbbuffer.server.compressing;

import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.schema.FieldDefinition;
import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.utils.FieldTypeUtils;

import java.io.IOException;

public interface Compressor<T> {

    void addRecord(Record<T> record) throws IOException;

    CompressedRecord<T> compress() throws IOException;

    void release();

    default void fillDefaultValue(Object[] values, FieldDefinition[] fieldDefinitions, long setMask, long unsetMask) {
        for (int i = 0, len = fieldDefinitions.length; i < len; i++) {
            FieldDefinition fieldDefinition = fieldDefinitions[i];
            long bitMask = fieldDefinition.getBitMask();
            if ((bitMask & unsetMask) == bitMask) {
                values[i] = FieldTypeUtils.getDefaultValue(fieldDefinition.getType());
            } else if ((bitMask & setMask) != bitMask) {
                values[i] = null;
            }
        }
    }

    default long getProtectMask() {
        return ~(1L << Constants.FIXED_ID_INDEX);
    }
}
