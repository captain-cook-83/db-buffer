package com.karma.dbbuffer.client.utils;

import com.google.protobuf.CodedOutputStream;
import com.karma.commons.utils.BytesUtils;
import com.karma.dbbuffer.client.entity.PEntity;
import com.karma.dbbuffer.schema.FieldDefinition;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class PEntityIncrementalSerializeUtils {

    public static void serialize(PEntity entity, long setBits, CodedOutputStream outputStream) throws IOException {
        FieldDefinition[] fieldDefinitions = entity.get_$fieldDefinitions();
        for (int i = 0, len = fieldDefinitions.length; i < len; i++) {
            FieldDefinition fieldDefinition = fieldDefinitions[i];
            if ((setBits & fieldDefinition.getBitMask()) != fieldDefinition.getBitMask()) {
                continue;
            }

            int index = fieldDefinition.getIndex();
            switch (fieldDefinition.getType()) {
                case BOOLEAN:
                    outputStream.writeRawByte((byte) (entity.get_$PrimitiveBoolean(index) ? 1 : 0));
                    break;
                case BYTE:
                    outputStream.writeRawByte(entity.get_$PrimitiveByte(index));
                    break;
                case SHORT:
                    outputStream.writeRawBytes(BytesUtils.convertFromShort(entity.get_$PrimitiveShort(index)));
                    break;
                case INT:
                    outputStream.writeInt32NoTag(entity.get_$PrimitiveInt(index));
                    break;
                case LONG:
                    outputStream.writeInt64NoTag(entity.get_$PrimitiveLong(index));
                    break;
                case STRING:
                    String stringObject = entity.get_$String(index);
                    if (stringObject != null) {
                        outputStream.writeStringNoTag(stringObject);
                    } else {
                        log.warn("unexpected field write {} -> index", entity.getClass(), index);
                    }
                    break;
                case INTEGER_O:
                    Integer intObject = entity.get_$Integer(index);
                    if (intObject != null) {
                        outputStream.writeInt32NoTag(intObject.intValue());
                    } else {
                        log.warn("unexpected field write {} -> index", entity.getClass(), index);
                    }
                    break;
                case LONG_O:
                    Long longObject = entity.get_$Long(index);
                    if (longObject != null) {
                        outputStream.writeInt64NoTag(longObject.longValue());
                    } else {
                        log.warn("unexpected field write {} -> index", entity.getClass(), index);
                    }
                    break;
                default:
                    log.warn("unknown field type {}", fieldDefinition.getType());
                    break;
            }
        }
    }
}
