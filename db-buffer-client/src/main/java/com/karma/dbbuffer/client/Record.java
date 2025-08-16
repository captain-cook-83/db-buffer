package com.karma.dbbuffer.client;

import com.karma.dbbuffer.PRecord;
import lombok.Data;

@Data
public class Record<T> extends PRecord<T> {

    private int dataLength;
}
