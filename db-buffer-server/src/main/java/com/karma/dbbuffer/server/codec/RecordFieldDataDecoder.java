package com.karma.dbbuffer.server.codec;

import com.google.protobuf.CodedInputStream;
import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.schema.FieldType;
import com.karma.dbbuffer.utils.FieldTypeUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class RecordFieldDataDecoder {

    private final CodedInputStream inputStream;

    public RecordFieldDataDecoder(byte[] data, int offset, int length) {
        inputStream = CodedInputStream.newInstance(data, offset, length);
    }

    public byte readByte() throws IOException {
        return inputStream.readRawByte();
    }

    public long readFieldMask(int maxIndex) throws IOException {
        if (maxIndex < Constants.MAX_SIZE_OF_SMALL_ENTITY) {
            return inputStream.readInt32();
        } else {
            return inputStream.readInt64();
        }
    }

//    public long readFieldMask(int totalFields) throws IOException {
//        if (totalFields > 32) {
//            return inputStream.readInt64();
//        } else if (totalFields > 16) {
//            return inputStream.readInt32();
//        } else if (totalFields > 8) {
//            return readShort();
//        } else {
//            return inputStream.readRawByte();
//        }
//    }

    public Object readObject(FieldType type) throws IOException {
        return FieldTypeUtils.readFromStreamByType(inputStream, type);
    }

    public void skip(FieldType type) throws IOException {
        if (type.isFixedBytes()) {
            inputStream.skipRawBytes(type.getFixedLength());
        } else {
            inputStream.skipField(type.getPBTag());
        }
    }
}
