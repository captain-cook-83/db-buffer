package com.karma.dbbuffer.client;

import com.google.protobuf.CodedOutputStream;
import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.client.codec.RecordSerializer;
import com.karma.dbbuffer.client.config.DBBufferWriterConfig;
import com.karma.dbbuffer.client.config.KafkaConfig;
import com.karma.dbbuffer.client.entity.PBigEntity;
import com.karma.dbbuffer.client.entity.PEntity;
import com.karma.dbbuffer.client.entity.PSmallEntity;
import com.karma.dbbuffer.client.utils.PEntityIncrementalSerializeUtils;
import com.karma.dbbuffer.schema.OperationType;
import com.karma.dbbuffer.schema.SchemaRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class BufferedDBWriter implements Callback {

    private DBBufferWriterConfig bufferedDBWriter;

    private KafkaConfig kafkaConfig;

    private KafkaProducer<Long, Record<Long>> producer;

    @Autowired
    protected void setDbBufferWriterConfig(DBBufferWriterConfig bufferedDBWriter) {
        this.bufferedDBWriter = bufferedDBWriter;
        this.kafkaConfig = bufferedDBWriter.getKafka();
    }

    @PostConstruct
    void onInit() throws InterruptedException, ExecutionException {
        SchemaRegistry.getInstance().loadSchemaDefinitions(bufferedDBWriter.getSchemaPath());

        LongSerializer keySerializer = new LongSerializer();
        LongSerializer idSerializer = new LongSerializer();
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, kafkaConfig.getRetryTimes());
        producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, kafkaConfig.getRetryBackoffMs());
        producerProps.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.getAcks());
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaConfig.getBufferMemory());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaConfig.getBatchSize());
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, kafkaConfig.getLingerMs());
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaConfig.getMaxBlockMs());
        this.producer = new KafkaProducer<>(producerProps, keySerializer, new RecordSerializer(idSerializer));
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.error("dbbuffer send exception {}", metadata, exception);
        } else {
            log.debug("dbbuffer send complete {}", metadata);
        }
    }

    @PreDestroy
    void destroy() {
        producer.close(Duration.ofSeconds(5));
        log.info("BYE, DBBuffer!");
    }

    public void sendRecord(Long ownerId, Object object, byte operationType) {
        if (!(object instanceof PEntity)) {
            throw new IllegalArgumentException("object must bo of PEntity");
        }

        PEntity<Long> entity = (PEntity<Long>) object;
        Record<Long> record = createRecord(entity, operationType);
        entity._$clearAllMask();

        long start = System.currentTimeMillis();
        producer.send(new ProducerRecord<>(kafkaConfig.getTopic(), ownerId, record), this);      // synchronized inside
        long delay = System.currentTimeMillis() - start;
        if (delay > 1) {
            log.warn("Kafka Send Delay {}ms", delay);
        }
    }

    private Record<Long> createRecord(PEntity<Long> entity, byte operationType) {
        Record<Long> record = new Record<>();
        byte operationBits = (byte) (operationType << 1);

        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            CodedOutputStream outputStream = CodedOutputStream.newInstance(buffer);     //TODO buffer recycle
            outputStream.writeRawByte(entity.get_$schemaVersion());

            long setBits;
            if (entity instanceof PSmallEntity) {
                PSmallEntity smallEntity = (PSmallEntity) entity;
                int setMask = smallEntity.get_$setMask();
                if (operationType == OperationType.INSERT) {
                    setMask |= smallEntity.get_$insertMask() & ~smallEntity.get_$unsetMask();
                }

                int unsetMask = smallEntity.get_$unsetMask();
                if (unsetMask != 0) {
                    outputStream.writeRawByte(operationBits | 0x01);
                    outputStream.writeInt32NoTag(setMask);
                    outputStream.writeInt32NoTag(unsetMask);
                } else {
                    outputStream.writeRawByte(operationBits);
                    outputStream.writeInt32NoTag(setMask);
                }
                setBits = setMask;
            } else {
                PBigEntity bigEntity = (PBigEntity) entity;
                long setMask = bigEntity.get_$setMask();
                if (operationType == OperationType.INSERT) {
                    setMask |= bigEntity.get_$insertMask() & ~bigEntity.get_$unsetMask();
                }

                long unsetMask = bigEntity.get_$unsetMask();
                if (unsetMask != 0) {
                    outputStream.writeRawByte(operationBits | 0x01);
                    outputStream.writeInt64NoTag(setMask);
                    outputStream.writeInt64NoTag(unsetMask);
                } else {
                    outputStream.writeRawByte(operationBits);
                    outputStream.writeInt64NoTag(setMask);
                }
                setBits = setMask;
            }

            PEntityIncrementalSerializeUtils.serialize(entity, setBits, outputStream);

            record.setDataLength(outputStream.getTotalBytesWritten());
            outputStream.flush();
            record.setData(buffer.toByteArray());
        } catch (IOException exception) {
            log.error("", exception);
        }

        record.setProtocolVersion((byte) Constants.NONE_VERSION);               // TODO 保留字段
        record.setCollection(entity.get_$schemaId());
        record.setId(entity.getId());
        record.setVersion(entity.getVersion());
        return record;
    }
}
