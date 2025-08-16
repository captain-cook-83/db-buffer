package com.karma.dbbuffer.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("acceptor")
public class AcceptorConfig {

    private KafkaConfig kafka;

    private int maxAcceptors;
}
