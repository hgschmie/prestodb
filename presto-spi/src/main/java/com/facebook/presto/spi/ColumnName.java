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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class ColumnName
{
    private final String columnName;

    private ColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    @JsonCreator
    public static ColumnName createColumnName(String columnName)
    {
        if (columnName == null || columnName.isEmpty()) {
            throw new NullPointerException("columnName is null or empty");
        }

        return new ColumnName(columnName);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
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
        return Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("ColumnName{");
        sb.append("columnName='").append(columnName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
