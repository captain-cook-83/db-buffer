package com.karma.dbbuffer.schema;

public interface OperationType {

    byte NONE = 0;

    byte UPDATE = 0x01;

    byte INSERT = UPDATE << 1;

    byte DELETE = INSERT << 1;

    static byte identify(byte operation) {
        boolean deleteOperation = (operation & DELETE) == DELETE;
        boolean insertOperation = (operation & INSERT) == INSERT;
        if (deleteOperation) {
            return insertOperation ? NONE : DELETE;
        } else {
            return insertOperation ? INSERT : UPDATE;
        }
    }
}
