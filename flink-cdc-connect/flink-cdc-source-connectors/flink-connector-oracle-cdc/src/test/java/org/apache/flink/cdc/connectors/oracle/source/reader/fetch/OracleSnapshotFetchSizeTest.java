/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies the fetch size passed by the snapshot task to JDBC. */
class OracleSnapshotFetchSizeTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 1024, 5000})
    void testConfiguredFetchSizeReachesJdbc(int fetchSize) throws Exception {
        verifyJdbcFetchSize(fetchSize, new Properties(), fetchSize);
    }

    @Test
    void testDebeziumSnapshotOverrideReachesJdbc() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("snapshot.fetch.size", "8000");
        verifyJdbcFetchSize(5000, properties, 8000);
    }

    @SuppressWarnings("unchecked")
    private void verifyJdbcFetchSize(int fetchSize, Properties properties, int expected)
            throws Exception {
        properties.setProperty("query.fetch.size", "17");
        OracleSourceConfigFactory factory = new OracleSourceConfigFactory();
        factory.hostname("localhost");
        factory.databaseList("MYDB");
        factory.username("user");
        factory.password("password");
        factory.fetchSize(fetchSize);
        factory.debeziumProperties(properties);
        OracleSourceConfig config = factory.create(0);

        TableId tableId = new TableId("MYDB", "TEST", "PRODUCTS");
        Table table =
                Table.editor()
                        .tableId(tableId)
                        .addColumn(
                                Column.editor()
                                        .name("ID")
                                        .position(1)
                                        .jdbcType(Types.INTEGER)
                                        .create())
                        .setPrimaryKeyNames("ID")
                        .create();
        SnapshotSplit split =
                new SnapshotSplit(
                        tableId,
                        0,
                        (RowType)
                                DataTypes.ROW(DataTypes.FIELD("ID", DataTypes.INT()))
                                        .getLogicalType(),
                        null,
                        null,
                        null,
                        Collections.emptyMap());
        OracleConnection jdbc = mock(OracleConnection.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(jdbc.connection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnName(1)).thenReturn("ID");
        when(metadata.getColumnLabel(1)).thenReturn("ID");
        OracleDatabaseSchema schema = mock(OracleDatabaseSchema.class);
        when(schema.tableFor(tableId)).thenReturn(table);
        EventDispatcher<OraclePartition, TableId> dispatcher = mock(EventDispatcher.class);
        EventDispatcher.SnapshotReceiver<OraclePartition> receiver =
                mock(EventDispatcher.SnapshotReceiver.class);
        when(dispatcher.getSnapshotChangeEventReceiver()).thenReturn(receiver);
        SnapshotProgressListener<OraclePartition> listener = mock(SnapshotProgressListener.class);
        OracleOffsetContext offset = mock(OracleOffsetContext.class);
        OracleScanFetchTask.OracleSnapshotSplitReadTask task =
                new OracleScanFetchTask.OracleSnapshotSplitReadTask(
                        config.getDbzConnectorConfig(),
                        offset,
                        listener,
                        schema,
                        jdbc,
                        dispatcher,
                        split);

        assertThat(
                        task.execute(() -> true, new OraclePartition("test"), offset)
                                .isCompletedOrSkipped())
                .isTrue();

        verify(statement).setFetchSize(expected);
        verify(statement).executeQuery();
        verify(statement).close();
        verify(resultSet).close();
        verify(receiver).completeSnapshot();
    }
}
