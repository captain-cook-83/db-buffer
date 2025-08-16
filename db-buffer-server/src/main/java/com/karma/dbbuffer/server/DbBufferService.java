package com.karma.dbbuffer.server;

import com.karma.dbbuffer.server.components.RecordAcceptor;
import com.karma.dbbuffer.server.components.RecordProcessor;
import com.karma.dbbuffer.server.components.RecordProcessorFactory;
import com.karma.dbbuffer.server.compressing.CompressorFactory;
import com.karma.dbbuffer.server.compressing.simple.SimpleCompressor;
import com.karma.dbbuffer.server.compressing.sorting.SortableCompressor;
import com.karma.dbbuffer.server.persisting.DatabaseBatchWriter;
import com.karma.dbbuffer.server.persisting.JDBCDatabaseBatchWriter;
import com.karma.dbbuffer.server.recycling.ResourceRecycler;
import com.karma.dbbuffer.server.config.AcceptorConfig;
import com.karma.dbbuffer.server.config.ProcessorConfig;
import com.karma.dbbuffer.server.recycling.ArrayRecycler;
import com.karma.dbbuffer.schema.SchemaRegistry;
import com.karma.dbbuffer.schema.VersionableSchema;
import com.karma.dbbuffer.server.serializing.RecordsSQLSerializer;
import com.karma.dbbuffer.server.serializing.RecordsSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.util.Set;
import java.util.concurrent.*;

@Slf4j
@Service
public class DbBufferService {

    private AcceptorConfig acceptorConfig;

    private ProcessorConfig processorConfig;

    private DataSource dataSource;

    private ResourceRecycler resourceRecycler;

    @Autowired
    protected void setAcceptorConfig(AcceptorConfig acceptorConfig) {
        this.acceptorConfig = acceptorConfig;
    }

    @Autowired
    protected void setProcessorConfig(ProcessorConfig processorConfig) {
        this.processorConfig = processorConfig;
    }

    @Autowired
    protected void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Autowired
    protected void setResourceRecycler(ResourceRecycler resourceRecycler) {
        this.resourceRecycler = resourceRecycler;
    }

    private ExecutorService acceptors;

    @PostConstruct
    void onInit() throws InterruptedException, ExecutionException {
        if (SchemaRegistry.getInstance().loadSchemaDefinitions(processorConfig.getSchemaPath())) {
            int totalProcessors = Runtime.getRuntime().availableProcessors();
            int acceptorNum = Math.min(acceptorConfig.getMaxAcceptors(), totalProcessors * 2 + 1);
            acceptors = Executors.newFixedThreadPool(acceptorNum);

            int totalSchemas = SchemaRegistry.getInstance().getSchemas().size();
            Set<VersionableSchema> latestSchemas = SchemaRegistry.getInstance().getSchemas();
            RecordsSerializer<Long> recordsSerializer =
                    new RecordsSQLSerializer(processorConfig.getBatchWriteSize(), totalSchemas, processorConfig.getWarmupDuration());
            ArrayRecycler<Record> recordArrayRecycler = resourceRecycler.getRecordArrayRecycler();
            ArrayRecycler<Object> objectArrayRecycler = resourceRecycler.getObjectArrayRecycler();
            CompressorFactory<Long> compressorFactory = schema -> schema.isSortingOrder() ?
                    new SortableCompressor<>(schema, recordArrayRecycler, objectArrayRecycler, processorConfig.getSortingArrayCapacity()) :
                    new SimpleCompressor<>(schema, objectArrayRecycler);
            Deserializer<Long> keyDeserializer = new LongDeserializer();
            Deserializer<Long> idDeserializer = new LongDeserializer();
            DatabaseBatchWriter batchWriter = new JDBCDatabaseBatchWriter(dataSource, processorConfig.getBatchWriteSize());

            RecordProcessorFactory<Long> recordProcessorFactory = () ->
                    new RecordProcessor<>(processorConfig, latestSchemas, recordsSerializer, batchWriter, compressorFactory);

            for (int i = 0; i < acceptorNum; i++) {
                acceptors.submit(new RecordAcceptor<>(i, acceptorConfig, keyDeserializer, idDeserializer, recordProcessorFactory));
            }
        }
    }

    @PreDestroy
    void destroy() {
        acceptors.shutdownNow();
        resourceRecycler.dispose();
        log.info("BYE!");
    }
}
