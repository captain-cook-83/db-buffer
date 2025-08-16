package com.karma.dbbuffer.client.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("db-buffer")
public class DBBufferWriterConfig {

    private String schemaPath;

    private KafkaConfig kafka;
}
