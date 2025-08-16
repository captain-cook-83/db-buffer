package com.karma.dbbuffer.server.serializing;

import com.karma.dbbuffer.server.compressing.BatchRecords;

import java.util.LinkedList;

public interface RecordsSerializer<T> {

    LinkedList<BatchStatement> serialize(BatchRecords<T> records);
}
