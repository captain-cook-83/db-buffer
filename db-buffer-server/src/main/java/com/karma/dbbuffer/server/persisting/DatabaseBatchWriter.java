package com.karma.dbbuffer.server.persisting;

import com.karma.dbbuffer.server.serializing.BatchStatement;

import java.util.LinkedList;
import java.util.List;

public interface DatabaseBatchWriter {

    void write(List<LinkedList<BatchStatement>> statements);
}
