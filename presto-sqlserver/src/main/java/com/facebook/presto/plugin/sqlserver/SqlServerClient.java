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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

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
}
