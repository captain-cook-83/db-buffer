package com.karma.dbbuffer.utils;

import com.google.protobuf.CodedInputStream;
import com.karma.commons.utils.BytesUtils;
import com.karma.commons.values.NullValue;
import com.karma.dbbuffer.schema.FieldType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class FieldTypeUtils {

    public static Object getDefaultValue(FieldType fieldType) {
        switch (fieldType) {
            case BOOLEAN:
                return false;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return 0;
            case STRING:
            case INTEGER_O:
            case LONG_O:
                return new NullValue();
        }
        return null;
    }

    public static Object readFromStreamByType(CodedInputStream inputStream, FieldType type) throws IOException {
        switch (type) {                                 // TODO more type
            case BOOLEAN:
                return inputStream.readRawByte() != 0;
            case BYTE:
                return inputStream.readRawByte();
            case SHORT:
                return readShort(inputStream);
            case INT:
                return inputStream.readInt32();
            case LONG:
                return inputStream.readInt64();
            case STRING:
                return inputStream.readString();
            case INTEGER_O:
                return Integer.valueOf(inputStream.readInt32());
            case LONG_O:
                return Long.valueOf(inputStream.readInt64());
            default:
                log.warn("unknown field type {}", type);
                break;
        }
        return null;
    }

    public static void setParamByType(
            PreparedStatement preparedStatement, int index, Object param, FieldType type) throws SQLException {
        switch (type) {
            case BOOLEAN:
                preparedStatement.setBoolean(index, (Boolean) param);
                break;
            case BYTE:
                preparedStatement.setByte(index, (Byte) param);
                break;
            case SHORT:
                preparedStatement.setShort(index, (Short) param);
                break;
            case INT:
            case INTEGER_O:
            case LONG:
            case LONG_O:
                preparedStatement.setObject(index, param);
                break;
            case STRING:
                preparedStatement.setString(index, (String) param);
                break;
        }
    }

    private static short readShort(CodedInputStream inputStream) throws IOException {
        return BytesUtils.convertToShort(inputStream.readRawBytes(2), 0);
    }
}
