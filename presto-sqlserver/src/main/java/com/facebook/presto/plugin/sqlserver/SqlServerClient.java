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
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.*;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;

public class SqlServerClient
    extends BaseJdbcClient
{
  @Inject
  public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
      throws SQLException
  {
    super(connectorId, config, "\"", new SQLServerDriver());
  }

  @Override
  public Set<String> getSchemaNames()
  {
    // for SqlServer, we need to list catalogs instead of schemas
    try (Connection connection = driver.connect(connectionUrl, connectionProperties);
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
      throw Throwables.propagate(e);
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
        new String[] {"TABLE"});
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

  protected Type toPrestoType(int jdbcType, int columnSize)
  {
    switch (jdbcType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return BOOLEAN;
      case Types.TINYINT:
        return TINYINT;
      case Types.SMALLINT:
        return SMALLINT;
      case Types.INTEGER:
        return INTEGER;
      case Types.BIGINT:
        return BIGINT;
      case Types.REAL:
        return REAL;
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return DOUBLE;
      case Types.CHAR:
      case Types.NCHAR:
        return createCharType(min(columnSize, CharType.MAX_LENGTH));
      case Types.VARCHAR:
      case Types.NVARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
        if (columnSize > VarcharType.MAX_LENGTH) {
          return createUnboundedVarcharType();
        }
        return createVarcharType(columnSize);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return VARBINARY;
      case Types.DATE:
        return DATE;
      case Types.TIME:
        return TIME;
      case Types.TIMESTAMP:
        return TIMESTAMP;
      case -155:
        return TIMESTAMP;
    }
    return null;
  }
}
