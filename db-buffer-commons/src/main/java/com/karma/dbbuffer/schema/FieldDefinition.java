package com.karma.dbbuffer.schema;

import lombok.Data;

@Data
public class FieldDefinition {

    private String fixedName;

    private int index;

    private long bitMask;

    private FieldType type;

    private boolean nullable;

    public FieldDefinition() {}

    public FieldDefinition(String fixedName, int index, FieldType type, boolean nullable) {
        this.fixedName = fixedName;
        this.index = index;
        this.type = type;
        this.nullable = nullable;

        this.bitMask = 1L << index;
    }
}
