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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.airlift.log.Logger;

import java.util.Objects;

import static com.facebook.presto.spi.SchemaUtil.checkNotEmpty;
import static java.util.Locale.ENGLISH;

public class SchemaTableName
{
    private static final Logger log = Logger.get(SchemaTableName.class);
    private String originSchemaName;
    private String originTableName;
    private String schemaName;
    private String tableName;

    @JsonCreator
    public static SchemaTableName valueOf(String schemaTableName)
    {
        checkNotEmpty(schemaTableName, "schemaTableName");
        String[] parts = schemaTableName.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid schemaTableName " + schemaTableName);
        }
        return new SchemaTableName(parts[0], parts[1]);
    }

    public SchemaTableName(String schemaName, String tableName)
    {
        setName(schemaName, tableName);
    }

    public void setName(String schemaName, String tableName)
    {
      this.originSchemaName = schemaName;
      this.originTableName = tableName;
      if (schemaName == null || tableName == null) {
        Exception err = new Exception("Null");
        log.error(err, "schemaName or tableName is null: " + schemaName + "," + tableName);
      }
      this.schemaName = checkNotEmpty(schemaName, "schemaName").toLowerCase(ENGLISH).replace("-", "_");
      this.tableName = checkNotEmpty(tableName, "tableName").toLowerCase(ENGLISH);
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getOriginSchemaName()
    {
      return originSchemaName;
    }

    public String getOriginTableName()
  {
    return originTableName;
  }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SchemaTableName other = (SchemaTableName) obj;
        return (Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName)) ||
            (Objects.equals(this.originSchemaName, other.originSchemaName) &&
                Objects.equals(this.originTableName, other.originTableName));
    }

    @JsonValue
    @Override
    public String toString()
    {
        return originSchemaName + '.' + originTableName;
    }

    public SchemaTablePrefix toSchemaTablePrefix()
    {
        return new SchemaTablePrefix(schemaName, tableName);
    }
}
