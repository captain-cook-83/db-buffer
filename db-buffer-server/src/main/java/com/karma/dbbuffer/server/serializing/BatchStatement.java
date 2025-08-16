package com.karma.dbbuffer.server.serializing;

import com.karma.dbbuffer.schema.VersionableSchema;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class BatchStatement {

    private static final String UPDATE_PREFIX = "UPDATE ";

    private VersionableSchema schema;

    private String statement;

    private List<Object[]> batchParams;

    public boolean isUpdate() { return statement.startsWith(UPDATE_PREFIX); }
}
