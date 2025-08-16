package com.karma.dbbuffer.server.components;

import com.karma.dbbuffer.server.Record;
import com.karma.dbbuffer.server.codec.RecordDeserializer;
import com.karma.dbbuffer.server.config.AcceptorConfig;
import com.karma.dbbuffer.server.config.KafkaConfig;
import com.karma.commons.utils.MapUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.*;

@Slf4j
public class RecordAcceptor<T> implements Runnable, ConsumerRebalanceListener, OffsetCommitCallback {

    private int index;

    private String topic;

    private RecordProcessorFactory<T> recordProcessorFactory;

    private KafkaConsumer<T, Record<T>> kafkaConsumer;

    private Map<Integer, RecordProcessor<T>> recordProcessors = new HashMap<>(4);

    public RecordAcceptor(int index, AcceptorConfig config,
                          Deserializer<T> keyDeserializer, Deserializer<T> idDeserializer, RecordProcessorFactory<T> recordProcessorFactory) {
        this.index = index;
        this.recordProcessorFactory = recordProcessorFactory;

        KafkaConfig kafkaConfig = config.getKafka();
        this.topic = kafkaConfig.getTopic();

        Properties consumerProps = new Properties();
        consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaConfig.getSessionTimeMs());
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafkaConfig.getHeartbeatIntervalMs());
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaConfig.getMaxPollIntervalMs());
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConfig.getMaxPollRecords());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getGroupId());
//        consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "Acceptor-" + index);           // https://kafka.apache.org/26/documentation.html#group.instance.id
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumer = new KafkaConsumer<>(consumerProps, keyDeserializer, new RecordDeserializer<>(idDeserializer));
        kafkaConsumer.subscribe(Arrays.asList(topic), this);

    }

    @Override
    public void run() {
        Thread.currentThread().setName("RecordAcceptor-" + index);
//        try {
//            Thread.sleep(1000L * (index % 5));
//        } catch (InterruptedException exception) {
//            log.warn("acceptor sleep exception", exception);
//        }

        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<T, Record<T>> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(5));
            try {
                for (ConsumerRecord<T, Record<T>> consumerRecord : consumerRecords) {
                    Record<T> record = consumerRecord.value();
                    if (record == null) {
                        log.warn("empty record value for {}", consumerRecord.key());
                        continue;
                    }

                    record.setOwnerId(consumerRecord.key());
                    record.setOffset(consumerRecord.offset());
                    record.setTimestamp(consumerRecord.timestamp());
                    log.info("receive record Offset({}) OwnerId({}) Collection({}) Id({})", record.getOffset(), record.getOwnerId(), record.getCollection(), record.getId());

                    int partition = consumerRecord.partition();
                    RecordProcessor<T> recordProcessor = recordProcessors.get(partition);
                    if (recordProcessor == null) {
                        recordProcessor = recordProcessors.computeIfAbsent(partition, p -> recordProcessorFactory.createRecordProcessor());
                        log.warn("unexpected partition received {}", partition);
                    }

                    long offset = recordProcessor.addRecord(record);
                    if (offset > 0) {
                        commitOffset(partition, offset);
                    }
                }

                for (Map.Entry<Integer, RecordProcessor<T>> entry : recordProcessors.entrySet()) {
                    long offset = entry.getValue().tickForPersisting();
                    if (offset > 0) {
                        commitOffset(entry.getKey(), offset);
                    }
                }
            } catch (Exception exception) {
                log.error("record process exception", exception);         //TODO auto repair
            }
        }

        dispose();
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            log.error("offsets commit exception {}", offsets, exception);
        } else {
            log.debug("offsets commit complete {}", offsets);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            RecordProcessor<T> recordProcessor = recordProcessors.remove(partition.partition());
            if (recordProcessor != null) {
                recordProcessor.dispose();
            }
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            recordProcessors.computeIfAbsent(partition.partition(), p -> recordProcessorFactory.createRecordProcessor());
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) { onPartitionsRevoked(partitions); }

    public void dispose() {
        kafkaConsumer.close();
        if (!recordProcessors.isEmpty()) {
            recordProcessors.values().forEach(p -> p.dispose());
            recordProcessors.clear();
        }
        log.info("acceptor terminated");
    }

    private void commitOffset(Integer partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        kafkaConsumer.commitAsync(MapUtils.createSingle(topicPartition, new OffsetAndMetadata(offset + 1)), this);
        log.debug("committing offset {}", topicPartition);
    }
}
