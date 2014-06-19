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
package com.facebook.presto.kafka;

import static com.google.common.base.Preconditions.checkNotNull;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.primitives.Ints;

public final class KafkaColumnHandle
    implements ConnectorColumnHandle, Comparable<KafkaColumnHandle>
{
    private final String connectorId;
    private final String columnName;
    private final String mapping;
    private final Type columnType;
    private final int ordinalPosition;

    public KafkaColumnHandle(String connectorId, String mapping, ColumnMetadata columnMetadata)
    {
        this(connectorId, columnMetadata.getName(), mapping, columnMetadata.getType(), columnMetadata.getOrdinalPosition());
    }

    @JsonCreator
    public KafkaColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        this.mapping = checkNotNull(mapping, "mapping is null");
        this.columnType = checkNotNull(columnType, "columnType is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public String getMapping()
    {
        return mapping;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, ordinalPosition, false);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, columnName, mapping, columnType, ordinalPosition);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        KafkaColumnHandle other = (KafkaColumnHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) &&
                Objects.equal(this.columnName, other.columnName) &&
                Objects.equal(this.mapping, other.mapping) &&
                Objects.equal(this.columnType, other.columnType) &&
                Objects.equal(this.ordinalPosition, other.ordinalPosition);
    }

    @Override
    public int compareTo(KafkaColumnHandle otherHandle)
    {
        return Ints.compare(this.getOrdinalPosition(), otherHandle.getOrdinalPosition());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("mapping", mapping)
                .add("columnType", columnType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
