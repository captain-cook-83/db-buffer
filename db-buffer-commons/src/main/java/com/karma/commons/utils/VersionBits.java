package com.karma.commons.utils;

interface VersionBits {

    enum Status {
        ORDERED, DISORDERED, DUPLICATED, LOST, OVERFLOW
    }

    long getHighestVersion();

    VersionBits getMissingVersions();

    void clearVersion(long version);

    Status setVersion(long version);
}
