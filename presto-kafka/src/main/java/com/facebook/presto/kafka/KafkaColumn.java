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
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public final class KafkaColumn
{
    private final String name;
    private final Type type;
    private final String mapping;
    private final String decoder;
    private final String format;

    @JsonCreator
    public KafkaColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("decoder") String decoder,
            @JsonProperty("format") String format)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        checkArgument(!isNullOrEmpty(mapping), "mapping is null or is empty");
        this.name = name;
        this.type = checkNotNull(type, "type is null");
        this.mapping = mapping;
        this.decoder = decoder;
        this.format = format;
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
    public String getDecoder()
    {
        return decoder;
    }

    @JsonProperty
    public String getFormat()
    {
        return format;
    }

    public ColumnMetadata getColumnMetadata(int index)
    {
        return new ColumnMetadata(name, type, index, false);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type, mapping, decoder, format);
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

        KafkaColumn other = (KafkaColumn) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.mapping, other.mapping) &&
                Objects.equal(this.decoder, other.decoder) &&
                Objects.equal(this.format, other.format);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("mapping", mapping)
                .toString();
    }
}
