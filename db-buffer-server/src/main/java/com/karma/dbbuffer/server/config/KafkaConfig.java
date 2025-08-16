package com.karma.dbbuffer.server.config;

import lombok.Data;

@Data
public class KafkaConfig {

    private String bootstrapServers;

    private String topic;

    private String groupId;

    private int sessionTimeMs;

    private int heartbeatIntervalMs;

    private int maxPollIntervalMs;

    private int maxPollRecords;
}
