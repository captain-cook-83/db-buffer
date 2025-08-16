package com.karma.dbbuffer.server.compressing;

import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

@Getter
public class BatchRecords<T> {

    private LinkedList<CompressedRecord<T>> newRecords;

    private LinkedList<CompressedRecord<T>> changedRecords;

    private LinkedList<T> deletedIds;

    private long lastOffset;

    private Byte collection;

    public BatchRecords(LinkedList<CompressedRecord<T>> newRecords, LinkedList<CompressedRecord<T>> changedRecords, LinkedList<T> deletedIds) {
        this.newRecords = newRecords;
        this.changedRecords = changedRecords;
        this.deletedIds = deletedIds;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public void setCollection(Byte collection) {
        this.collection = collection;
    }

    public void release() {
        newRecords.forEach(r -> r.release());
        changedRecords.forEach(r -> r.release());
    }
}
