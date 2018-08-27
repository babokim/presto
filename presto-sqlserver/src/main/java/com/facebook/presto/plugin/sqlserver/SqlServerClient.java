/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableSet;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.bigintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.booleanReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.charReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.dateReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.decimalReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.doubleReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.integerReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.realReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.smallintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.timeReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.timestampReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.tinyintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varbinaryReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varcharReadMapping;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;

public class SqlServerClient
        extends BaseJdbcClient
{
    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", connectionFactory(config));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        return new DriverConnectionFactory(new SQLServerDriver(), config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public Set<String> getSchemaNames()
    {
        // for SqlServer, we need to list catalogs instead of schemas
        try (Connection connection = connectionFactory.openConnection();
                ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("master")
                        && !schemaName.equals("model")
                        && !schemaName.equals("msdb")
                        && !schemaName.equals("tempdb")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        // SqlServer maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                schemaName,
                null,
                escapeNamePattern(tableName, escape),
                new String[]{"TABLE"});
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        // SqlServer uses catalogs instead of schemas
        return new SchemaTableName(
                resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanReadMapping());
            case Types.TINYINT:
                return Optional.of(tinyintReadMapping());
            case Types.SMALLINT:
                return Optional.of(smallintReadMapping());
            case Types.INTEGER:
                return Optional.of(integerReadMapping());
            case Types.BIGINT:
                return Optional.of(bigintReadMapping());
            case Types.REAL:
                return Optional.of(realReadMapping());
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleReadMapping());
            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalReadMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(charReadMapping(createCharType(charLength)));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryReadMapping());
            case Types.DATE:
                return Optional.of(dateReadMapping());
            case Types.TIME:
                return Optional.of(timeReadMapping());
            case Types.TIMESTAMP:
                return Optional.of(timestampReadMapping());
            case -155:
                return Optional.of(timestampReadMapping());
        }
        return Optional.empty();
    }
}
