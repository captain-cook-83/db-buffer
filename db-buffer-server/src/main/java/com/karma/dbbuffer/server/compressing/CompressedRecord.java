package com.karma.dbbuffer.server.compressing;

import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.server.recycling.ArrayRecycler;
import lombok.Getter;

@Getter
public class CompressedRecord<T> {

    private ArrayRecycler<Object> valuesRecycler;

    private byte operation;

    private long version;

    private long bitsMask;

    private Object[] values;

    public CompressedRecord(ArrayRecycler<Object> valuesRecycler, byte operation, long version, long bitsMask, Object[] values) {
        this.valuesRecycler = valuesRecycler;
        this.operation = operation;
        this.version = version;
        this.bitsMask = bitsMask;
        this.values = values;
    }

    public void setId(T value) {
        values[Constants.FIXED_ID_INDEX] = value;
    }

    public T getId() {
        return (T) values[Constants.FIXED_ID_INDEX];
    }

    public void release() {
        valuesRecycler.recycle(values);
        values = null;
    }
}
