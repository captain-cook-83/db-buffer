package com.karma.dbbuffer.server;

import com.karma.dbbuffer.PRecord;
import lombok.Data;

@Data
public class Record<T> extends PRecord<T> {

    private T ownerId;

    private long offset;

    private long timestamp;

    private int dataOffset;
}
