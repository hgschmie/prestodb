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
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Kafka specific connector column handle.
 */
public final class KafkaColumnHandle
        implements ConnectorColumnHandle, Comparable<KafkaColumnHandle>
{
    private final String connectorId;
    private final int ordinalPosition;

    /**
     * Column Name
     */
    private final String name;

    /**
     * Column type
     */
    private final Type type;

    /**
     * Mapping hint for the decoder. Can be null.
     */
    private final String mapping;

    /**
     * Data format to use (selects the decoder). Can be null.
     */
    private final String groupDataFormat;

    /**
     * Data format to use (selects the decoder). Can be null.
     */
    private final String fieldDataFormat;

    /**
     * Additional format hint for the selected decoder. Selects a decoder subtype (e.g. which timestamp decoder).
     */
    private final String formatHint;

    /**
     * True if the column should be hidden.
     */
    private final boolean hidden;

    /**
     * True if the column is internal to the connector and not defined by a topic definition.
     */
    private final boolean internal;

    @JsonCreator
    public KafkaColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("groupDataFormat") String groupDataFormat,
            @JsonProperty("fieldDataFormat") String fieldDataFormat,
            @JsonProperty("formatHint") String formatHint,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("internal") boolean internal)

    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.ordinalPosition = ordinalPosition;
        this.name = checkNotNull(name, "name is null");
        this.type = checkNotNull(type, "type is null");
        this.mapping = mapping;
        this.groupDataFormat = groupDataFormat;
        this.fieldDataFormat = fieldDataFormat;
        this.formatHint = formatHint;
        this.hidden = hidden;
        this.internal = internal;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getMapping()
    {
        return mapping;
    }

    @JsonProperty
    public String getGroupDataFormat()
    {
        return groupDataFormat;
    }

    @JsonProperty
    public String getFieldDataFormat()
    {
        return fieldDataFormat;
    }

    @JsonProperty
    public String getFormatHint()
    {
        return formatHint;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(getName(), getType(), getOrdinalPosition(), isHidden());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, ordinalPosition, name, type, mapping, groupDataFormat, fieldDataFormat, formatHint, hidden, internal);
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

        KafkaColumnHandle other = (KafkaColumnHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) &&
                Objects.equal(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.mapping, other.mapping) &&
                Objects.equal(this.groupDataFormat, other.groupDataFormat) &&
                Objects.equal(this.fieldDataFormat, other.fieldDataFormat) &&
                Objects.equal(this.formatHint, other.formatHint) &&
                Objects.equal(this.hidden, other.hidden) &&
                Objects.equal(this.internal, other.internal);
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
                .add("name", name)
                .add("type", type)
                .add("mapping", mapping)
                .add("groupDataFormat", groupDataFormat)
                .add("fieldDataFormat", fieldDataFormat)
                .add("formatHint", formatHint)
                .add("hidden", hidden)
                .add("internal", internal)
                .toString();
    }
}
