package com.karma.dbbuffer.client.codec;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.CodedOutputStream;
import com.karma.dbbuffer.client.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

//TODO
@Slf4j
public class RecordSerializer<T> implements Serializer<Record<T>> {

    private Serializer<T> idSerializer;

    public RecordSerializer(Serializer<T> idSerializer) {
        this.idSerializer = idSerializer;
    }

    @Override
    public byte[] serialize(String topic, Record<T> record) {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {              //TODO memory recycle
            CodedOutputStream outputStream = CodedOutputStream.newInstance(buffer);
            outputStream.writeRawByte(record.getProtocolVersion());
            outputStream.writeRawByte(record.getCollection());

            byte[] idData = idSerializer.serialize(topic, record.getId());
            outputStream.writeRawByte((byte) idData.length);
            outputStream.writeRawBytes(idData);
            outputStream.writeInt64NoTag(record.getVersion());
            outputStream.writeRawBytes(record.getData(), 0, record.getDataLength());
            outputStream.flush();
            return buffer.toByteArray();
        } catch (IOException exception) {
            log.error("Kafka value serialize exception: JSON-{}", JSON.toJSONString(record), exception);
            return null;
        }
    }
}
