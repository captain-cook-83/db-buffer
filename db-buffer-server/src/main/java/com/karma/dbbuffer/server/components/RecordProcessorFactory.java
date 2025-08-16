package com.karma.dbbuffer.server.components;

public interface RecordProcessorFactory<T> {

    RecordProcessor<T> createRecordProcessor();
}
