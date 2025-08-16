package com.karma.dbbuffer.schema;

import com.google.protobuf.WireFormat;

public enum FieldType {

    BOOLEAN(0),
    BYTE(1),
    SHORT(2),
    INT(3),
    LONG(4),
    STRING(5),
    INTEGER_O(6),
    LONG_O(7);

    private static final int RAW_SPACE = 8;

    private static final boolean[] PRIMITIVE_TYPES = new boolean[] { true, true, true, true, true, false, false, false };

    private static final int[] PB_TAGS = new int[] { 1 << RAW_SPACE, 1 << RAW_SPACE, 2 << RAW_SPACE,
            WireFormat.WIRETYPE_VARINT, WireFormat.WIRETYPE_VARINT, WireFormat.WIRETYPE_LENGTH_DELIMITED,
            WireFormat.WIRETYPE_VARINT, WireFormat.WIRETYPE_VARINT };

    public static FieldType valueOfDef(String def) {
        switch (def) {
            case "boolean":
                return BOOLEAN;
            case "byte":
                return BYTE;
            case "short":
                return SHORT;
            case "int":
                return INT;
            case "long":
                return LONG;
            case "String":
                return STRING;
            case "Integer":
                return INTEGER_O;
            case "Long":
                return LONG_O;
        }
        return null;
    }

    private int value;

    private FieldType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public boolean isPrimitiveType() { return PRIMITIVE_TYPES[value]; }

    public int getPBTag() { return PB_TAGS[value]; }

    public int getFixedLength() { return PB_TAGS[value] >> RAW_SPACE; }

    public boolean isFixedBytes() { return PB_TAGS[value] >= 1 << RAW_SPACE; }
}
