package com.karma.dbbuffer.schema;

import com.alibaba.fastjson.JSON;
import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.exception.InvalidSchemaException;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class SchemaRegistry {

    private static volatile SchemaRegistry instance;

    public static SchemaRegistry getInstance() {
        if (instance == null) {
            synchronized (SchemaRegistry.class) {
                if (instance == null) {
                    instance = new SchemaRegistry();
                }
            }
        }
        return instance;
    }

    private SchemaRegistry() {
        schemas = new HashMap<>();
    }

    private Map<Byte, MultiVersionableSchemas> schemas;

    public synchronized boolean loadSchemaDefinitions(String path) throws InterruptedException, ExecutionException {
        File schemaPath = new File(path);
        if (schemaPath.isDirectory()) {
            File[] files = schemaPath.listFiles(f -> f.getName().endsWith(".json"));
            if (files == null || files.length == 0) {
                log.error("empty schema config at {}", schemaPath.getAbsoluteFile());
                return false;
            }

            ExecutorService executorService = Executors.newFixedThreadPool(Math.min(5, files.length));
            CompletionService<VersionableSchema> completionService = new ExecutorCompletionService<>(executorService);

            for (File file : files) {
                File configFile = file;
                completionService.submit(() -> {
                    try (InputStream inputStream = new BufferedInputStream(new FileInputStream(configFile))) {
                        VersionableSchema schema = JSON.parseObject(inputStream, VersionableSchema.class);
                        String configFileName = configFile.getName();
                        if (!Objects.equals(configFileName, schema.getName() + "_" + schema.getVersion() + ".json")) {
                            log.warn("Inconsistent schema file name for {}", configFileName);
                        }
                        return schema;
                    }
                });
            }

            for (int i = 0; i < files.length; i++) {
                VersionableSchema schema = completionService.take().get();
                registrySchema(schema);
                log.info("Schema: {}->{}", schema.getName(), schema.getVersion());
            }
            executorService.shutdown();
            return true;
        } else {
            log.error("missing schema config path {}", schemaPath.getAbsoluteFile());
            return false;
        }
    }

    public synchronized void registrySchema(VersionableSchema schema) {
        schemas.computeIfAbsent(schema.getId(), id -> new MultiVersionableSchemas(schema)).addSchema(schema);
    }

    public VersionableSchema getSchema(Byte id, byte version) {
        MultiVersionableSchemas multiSchemas = schemas.get(id);
        if (multiSchemas != null) {
            return version < 0 ? multiSchemas.getSchema() : multiSchemas.getSchema(version);
        }

        throw new InvalidSchemaException(String.format("invalid schema id {}", id));
    }

    public VersionableSchema getSchema(Byte id) {
        return getSchema(id, (byte) Constants.NONE_VERSION);
    }

    public Set<VersionableSchema> getSchemas() {
        return schemas.values().stream().map(v -> v.getSchema()).collect(Collectors.toSet());
    }

    class MultiVersionableSchemas {

        private VersionableSchema current;              // speed up the most query

        private Map<Byte, VersionableSchema> compatibleSchemas;

        public MultiVersionableSchemas(VersionableSchema schema) {
            compatibleSchemas = new HashMap<>(3);
            current = schema;
        }

        public void addSchema(VersionableSchema schema) {
            compatibleSchemas.put(schema.getVersion(), schema);
            if (current.getVersion() < schema.getVersion()) {
                current = schema;
            }
        }

        public VersionableSchema getSchema(byte version) {
            if (current.getVersion() == version) {
                return current;
            }

            VersionableSchema schema = compatibleSchemas.get(version);
            if (schema != null) {
                return schema;
            }

            throw new InvalidSchemaException(String.format("invalid version {} for schema {}", version, current.getId()));
        }

        public VersionableSchema getSchema() {
            return current;
        }
    }
}
