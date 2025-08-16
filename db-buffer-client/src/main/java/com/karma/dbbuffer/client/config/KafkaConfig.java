package com.karma.dbbuffer.client.config;

import lombok.Data;

@Data
public class KafkaConfig {

    private String bootstrapServers;

    private String topic;

    private String acks;

    private int retryTimes;

    private int retryBackoffMs;

    private int bufferMemory;

    private int batchSize;

    private int lingerMs;

    private int maxBlockMs;
}
