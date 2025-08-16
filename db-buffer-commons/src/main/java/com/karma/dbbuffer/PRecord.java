package com.karma.dbbuffer;

import lombok.Data;

@Data
public class PRecord<T> {

    private byte protocolVersion;

    private Byte collection;

    private T id;

    private long version;

    private byte[] data;
}
