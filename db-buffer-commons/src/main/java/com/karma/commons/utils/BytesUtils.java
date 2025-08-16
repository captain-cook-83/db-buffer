package com.karma.commons.utils;

import com.karma.dbbuffer.exception.SerializationException;

public class BytesUtils {

    public static short convertToShort(byte[] data, int offset) {
        if (data == null || data.length - offset < Short.BYTES) {
            throw new SerializationException("remain size of data is less than " + Short.BYTES);
        }

        short value = 0;
        for (int i = offset, len = offset + Short.BYTES; i < len; i++) {
            value <<= 8;
            value |= data[i] & 0xFF;
        }
        return value;
    }

    public static byte[] convertFromShort(short data) {
        byte[] value = new byte[2];
        value[0] = (byte) (data >>> 8);
        value[1] = (byte) (data & 0x00FF);
        return value;
    }
}
