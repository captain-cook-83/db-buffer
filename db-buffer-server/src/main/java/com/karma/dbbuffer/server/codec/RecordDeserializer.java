package com.karma.dbbuffer.server.codec;

import com.google.protobuf.CodedInputStream;
import com.karma.dbbuffer.server.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Base64;

@Slf4j
public class RecordDeserializer<T> implements Deserializer<Record<T>> {

    private Deserializer<T> idDeserializer;

    public RecordDeserializer(Deserializer<T> idDeserializer) {
        this.idDeserializer = idDeserializer;
    }

    public Record<T> deserialize(String topic, byte[] data) {
        Record<T> record = new Record<>();
        CodedInputStream inputStream = CodedInputStream.newInstance(data);
        try {
            record.setProtocolVersion(inputStream.readRawByte());
            record.setCollection(inputStream.readRawByte());

            byte idLength = inputStream.readRawByte();
            record.setId(idDeserializer.deserialize(topic, inputStream.readRawBytes(idLength)));
            record.setVersion(inputStream.readInt64());
        } catch (IOException exception) {
            log.error("Kafka value deserialize exception: ({})Base64-{}", data.length, Base64.getEncoder().encodeToString(data), exception);
            return null;
        }

        record.setDataOffset(inputStream.getTotalBytesRead());
        record.setData(data);
        return record;
    }
}
