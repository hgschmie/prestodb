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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkNotNull;

public final class KafkaColumnHandle
        implements ConnectorColumnHandle, Comparable<KafkaColumnHandle>
{
    private final String connectorId;
    private final int ordinalPosition;
    private KafkaColumn kafkaColumn;

    @JsonCreator
    public KafkaColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("column") KafkaColumn kafkaColumn)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.ordinalPosition = ordinalPosition;
        this.kafkaColumn = checkNotNull(kafkaColumn, "kafkaColumn is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public KafkaColumn getColumn()
    {
        return kafkaColumn;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(kafkaColumn.toString(), kafkaColumn.getType(), ordinalPosition, false);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, ordinalPosition, kafkaColumn);
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
                Objects.equal(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equal(this.kafkaColumn, other.kafkaColumn);
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
                .add("ordinalPosition", ordinalPosition)
                .add("column", kafkaColumn)
                .toString();
    }
}
