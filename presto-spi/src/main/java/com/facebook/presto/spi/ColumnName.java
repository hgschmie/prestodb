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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class ColumnName
{
    public static final ColumnName EMPTY_COLUMN_NAME = new ColumnName("", false);

    // register quotes; according to https://en.wikibooks.org/wiki/SQL_Dialects_Reference/Data_structure_definition/Delimited_identifiers,
    // quoted identifiers must enforce case sensitivity.
    //
    // Not yet implemented.
    private final boolean quoted;

    private final String columnName;

    private ColumnName(String columnName, boolean quoted)
    {
        this.columnName = columnName;
        this.quoted = quoted;
    }

    @JsonCreator
    public static ColumnName createColumnName(String columnName)
    {
        if (columnName == null || columnName.isEmpty()) {
            throw new NullPointerException("columnName is null or empty");
        }

        return new ColumnName(columnName, false);
    }

    public static ColumnName createQuotedColumnName(String columnName, boolean quoted)
    {
        if (columnName == null || columnName.isEmpty()) {
            throw new NullPointerException("columnName is null or empty");
        }

        return new ColumnName(columnName, quoted);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonIgnore
    public boolean isQuoted()
    {
        return quoted;
    }

    @JsonIgnore
    public String getQuotedColumnName()
    {
        return "\"" + columnName + "\"";
    }

    public boolean sqlEquals(ColumnName other)
    {
        if (other == null) {
            return false;
        }

        if (quoted || other.isQuoted()) {
            return this.columnName.equals(other.getColumnName());
        }
        else {
            return this.columnName.equalsIgnoreCase(other.getColumnName());
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnName that = (ColumnName) o;
        return quoted == that.quoted &&
                Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(quoted, columnName);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("ColumnName{");
        sb.append("quoted=").append(quoted);
        sb.append(", columnName='").append(columnName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
