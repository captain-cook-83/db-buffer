package com.karma.dbbuffer.server.recycling;

import com.karma.dbbuffer.server.Record;
import org.springframework.stereotype.Component;

import java.util.Arrays;

//TODO
@Component
public class ResourceRecycler {

    public ArrayRecycler<Record> getRecordArrayRecycler() {
        return new ArrayRecycler<Record>() {

            @Override
            public Record[] get(int capacity) {
                return new Record[capacity];
            }

            @Override
            public void recycle(Record[] value) {
                Arrays.fill(value, null);
            }
        };
    }

    public ArrayRecycler<Object> getObjectArrayRecycler() {
        return new ArrayRecycler<Object>() {

            @Override
            public Object[] get(int capacity) {
                return new Object[capacity];
            }

            @Override
            public void recycle(Object[] value) {
                Arrays.fill(value, null);
            }
        };
    }

    public void dispose() {

    }
}
