package com.karma.commons.utils;

public class BitsUtils {

    public static final long HIGHEST_BIT_MASK = 0x8000000000000000L;

    public static final long FULL_BIT_MASK = 0xFFFFFFFFFFFFFFFFL;

    public static long getFullBits(long bits) {
        if (bits < HIGHEST_BIT_MASK) {
            long v = Long.highestOneBit(bits);
            return v - 1 + v;
        } else {
            return FULL_BIT_MASK;
        }
    }

    public static long setBit(long value, int index) {
        return value | (0x01L << index);
    }

    public static int setBit(int value, int index) {
        return value | (0x01 << index);
    }

    public static long clearBit(long value, int index) {
        return value & ~(0x01L << index);
    }

    public static int clearBit(int value, int index) {
        return value & ~(0x01 << index);
    }
}
