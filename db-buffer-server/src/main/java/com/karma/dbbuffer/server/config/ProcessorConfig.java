package com.karma.dbbuffer.server.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("processor")
public class ProcessorConfig {

    private String schemaPath;

    @Value("${processor.init.frame-interval-ms}")
    private int frameIntervalMs;

    @Value("${processor.init.frame-capacity}")
    private int frameCapacity;

    @Value("${processor.init.frame-init-records}")
    private int frameInitRecords;

    @Value("${processor.init.sorting-array-capacity}")
    private byte sortingArrayCapacity;                      // TODO adjust dynamically for each collection buffer

    @Value("${processor.persist.batch-write-size}")
    private int batchWriteSize;

    @Value("${processor.init.warmup-duration}")
    private int warmupDuration;
}
