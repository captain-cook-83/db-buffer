package com.karma.dbbuffer.server.persisting;

import com.alibaba.fastjson.JSON;
import com.karma.commons.values.NullValue;
import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.schema.FieldDefinition;
import com.karma.dbbuffer.server.serializing.BatchStatement;
import com.karma.dbbuffer.utils.FieldTypeUtils;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class JDBCDatabaseBatchWriter implements DatabaseBatchWriter {

    private DataSource dataSource;

    private int batchWriteSize;

    public JDBCDatabaseBatchWriter(DataSource dataSource, int batchWriteSize) {
        this.dataSource = dataSource;
        this.batchWriteSize = batchWriteSize;
    }

    @Override
    public void write(List<LinkedList<BatchStatement>> statements) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            for (LinkedList<BatchStatement> ss : statements) {
                Iterator<BatchStatement> iterator = ss.iterator();
                while (iterator.hasNext()) {
                    BatchStatement batchStatement = iterator.next();
                    boolean isUpdate = batchStatement.isUpdate();
                    FieldDefinition[] fieldDefinitions = batchStatement.getSchema().getFieldDefinitions();

                    try (PreparedStatement preparedStatement = connection.prepareStatement(batchStatement.getStatement())) {
                        log.debug("SQL: {}", batchStatement.getStatement());
                        int setParameterCount = preparedStatement.getParameterMetaData().getParameterCount();
                        if (isUpdate) {
                            setParameterCount -= 1;                                                                         // exclude the last id param for where segment
                        }

                        List<Object[]> batchParams = batchStatement.getBatchParams();
                        for (int i = 0, len = batchParams.size(); i < len; i++) {
                            Object[] params = batchParams.get(i);
                            if (params != null) {                                                                           // not a delete statement
                                log.debug("SQL PARAMS: {}", params);
                                int j = isUpdate ? Constants.FIXED_ID_INDEX + 1 : Constants.FIXED_ID_INDEX;                 // skip 0(id) for update segment
                                for (int paramIndex = 0, fLen = fieldDefinitions.length; paramIndex < setParameterCount && j < fLen; j++) {
                                    Object param = params[j];
                                    if (param != null) {
                                        FieldDefinition fieldDefinition = fieldDefinitions[j];
                                        FieldTypeUtils.setParamByType(preparedStatement, ++paramIndex,
                                                param instanceof NullValue ? null : param, fieldDefinition.getType());
                                    }
                                }
                                if (isUpdate) {       // set id param for where segment of update statement
                                    FieldTypeUtils.setParamByType(preparedStatement, setParameterCount + 1,
                                            params[Constants.FIXED_ID_INDEX], fieldDefinitions[Constants.FIXED_ID_INDEX].getType());
                                }
                            }

                            preparedStatement.addBatch();

                            if ((i + 1) % batchWriteSize == 0) {
                                preparedStatement.executeBatch();
                                connection.commit();
                                preparedStatement.clearBatch();
                            }
                        }

                        preparedStatement.executeBatch();
                        connection.commit();
                    }
                }
            }
        } catch (SQLException exception) {
            log.error("batch execute sql exception", exception);
        }
    }
}
