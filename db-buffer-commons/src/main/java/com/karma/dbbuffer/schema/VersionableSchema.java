package com.karma.dbbuffer.schema;

import lombok.Data;

@Data
public class VersionableSchema {

    private String name;

    private byte id;

    private byte version;

    private boolean sortingOrder;

    private FieldDefinition[] fieldDefinitions;

    private int maxIndex;

    public VersionableSchema() {}

    public VersionableSchema(String name, byte id, byte version, boolean sortingOrder, FieldDefinition[] fieldDefinitions) {
        this.name = name;
        this.id = id;
        this.version = version;
        this.sortingOrder = sortingOrder;
        this.fieldDefinitions = fieldDefinitions;

        this.maxIndex = fieldDefinitions[fieldDefinitions.length - 1].getIndex();
    }
}
