package com.karma.commons.utils;

class SimpleVersionBits implements VersionBits {

    private long bits;

    private long offset;

    public SimpleVersionBits(long version) {
        this(version, 0L);
    }

    private SimpleVersionBits(long offset, long bits) {
        this.offset = offset;
        this.bits = bits;
    }

    @Override
    public long getHighestVersion() {
        return offset + Long.SIZE - Long.numberOfLeadingZeros(bits);
    }

    @Override
    public VersionBits getMissingVersions() {
        long missingBits = bits ^ BitsUtils.getFullBits(bits);
        if (missingBits == 0) {
            return null;
        }

        SimpleVersionBits versionBits = new SimpleVersionBits(offset, missingBits);
        versionBits.clearVersion(offset);
        return versionBits;
    }

    @Override
    public void clearVersion(long version) {
        long value = version - offset;
        if (value > 0) {
            bits &= 0x01L << (value - 1);
        } else if (value == 0) {
            int adjustment = Long.numberOfTrailingZeros(bits) + 1;
            offset += adjustment;
            bits >>>= adjustment;
        }
    }

    @Override
    public Status setVersion(long version) {
        long value = version - offset;
        if (value > Long.SIZE) {
            return Status.OVERFLOW;
        }

        Status result = Status.DUPLICATED;
        if (value > 0) {
            value = 0x01L << (value - 1);
            if ((value & bits) == 0) {
                bits |= value;
                result = getStatus(value, bits);
            }
        } else if (value < 0) {                 // 发现前置版本，需要 offset
            value = Math.abs(value);
            if (Long.numberOfLeadingZeros(bits) < value) {
                return Status.OVERFLOW;
            } else {
                offset -= value;
                bits <<= value;
                result = value > 1 ? Status.LOST : Status.DISORDERED;
            }
        }

        return result;
    }

    private static Status getStatus(long index, long bits) {
        long v = BitsUtils.getFullBits(index);
        if ((bits & v) == v) {
            return bits > v ? Status.DISORDERED : Status.ORDERED;
        } else {
            return Status.LOST;
        }
    }
}
