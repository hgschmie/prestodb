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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class KafkaTable
{
    private final String tableName;
    private final String topicName;
    private final String decoderType;
    private final List<KafkaColumn> columns;

    @JsonCreator
    public KafkaTable(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("decoderType") String decoderType,
            @JsonProperty("columns") List<KafkaColumn> columns)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.topicName = checkNotNull(topicName, "topicName is null");
        this.decoderType = checkNotNull(decoderType, "decoderType is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public String getDecoderType()
    {
        return decoderType;
    }

    @JsonProperty
    public List<KafkaColumn> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .add("decoderType", decoderType)
                .add("columns", columns)
                .toString();
    }

    public static Function<KafkaTable, String> tableNameGetter()
    {
        return new Function<KafkaTable, String>()
        {
            @Override
            public String apply(KafkaTable table)
            {
                return table.getTableName();
            }
        };
    }
}
