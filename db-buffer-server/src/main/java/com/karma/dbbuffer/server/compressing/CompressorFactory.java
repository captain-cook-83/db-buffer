package com.karma.dbbuffer.server.compressing;

import com.karma.dbbuffer.schema.VersionableSchema;

public interface CompressorFactory<T> {

    Compressor<T> createCompressor(VersionableSchema schema);
}
