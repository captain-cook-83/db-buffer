package com.karma.dbbuffer.server.compressing;

import com.karma.dbbuffer.server.Record;

import java.io.IOException;

public interface Buffer<T> {

    boolean isEmpty();

    void addRecord(Record<T> record) throws IOException;

    BatchRecords<T> compress() throws IOException;

    void dispose();
}
