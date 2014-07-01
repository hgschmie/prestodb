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
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Json description to parse a message on a Kafka topic.
 * <p/>
 * <pre>
 * {
 * "tableName":"...table name visible to presto...",
 * "topicName":"...topic name read from kafka...",
 * "dataFormat":"message parse. Currently supported are 'json', 'csv' and 'dummy' (the default)",
 * "fields":[
 * {
 * "name":"... column name for this definition ...",
 * "type":"... any presto data type ...",
 * "mapping":"... a decoder specific mapping hint. For JSON, e.g. this can be a/b/c to look into nested dictionaries",
 * "dataFormat":"... select a field specific decoder for this column. Default is '_default'. See the decoder module documentation for details...",
 * "formatHint":"... additional format hinting to the selected decoder ...",
 * "hidden":"... boolean, true to hide the column in DESCRIBE TABLE and from SELECT *"
 * },
 * {...},
 * {...},
 * {...},
 * ...
 * }
 * }
 * </pre>
 *
 * TODO - rewrite this to have dataFormat and the fields inside a "value" dictionary and add a "key" dictionary that allows similar parsing of the
 * key. Add a decoder type "raw" which can e.g. take the raw long bytes of a long key and turn it into a value.
 */
public class KafkaTopicDescription
{
    private final String tableName;
    private final String topicName;
    private final String dataFormat;
    private final List<KafkaTopicFieldDescription> fields;

    @JsonCreator
    public KafkaTopicDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("fields") List<KafkaTopicFieldDescription> fields)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.topicName = checkNotNull(topicName, "topicName is null");
        this.dataFormat = checkNotNull(dataFormat, "dataFormat is null");
        this.fields = ImmutableList.copyOf(checkNotNull(fields, "fields is null"));
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
    public String getDataFormat()
    {
        return dataFormat;
    }

    @JsonProperty
    public List<KafkaTopicFieldDescription> getFields()
    {
        return fields;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tableName", tableName)
                .add("topicName", topicName)
                .add("dataFormat", dataFormat)
                .add("fields", fields)
                .toString();
    }
}
