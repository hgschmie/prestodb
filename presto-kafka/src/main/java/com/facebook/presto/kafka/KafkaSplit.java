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

import java.util.List;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public class KafkaSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String topicName;
    private final int partitionId;
    private final long start;
    private final long end;
    private final List<HostAddress> nodes;

    @JsonCreator
    public KafkaSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("topicName") String topicName,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("start") long start,
            @JsonProperty("end") long end,
            @JsonProperty("nodes") List<HostAddress> nodes)
    {
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.topicName = checkNotNull(topicName, "topicName is null");
        this.partitionId = partitionId;
        this.start = start;
        this.end = end;
        this.nodes = ImmutableList.copyOf(checkNotNull(nodes, "addresses is null"));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getEnd()
    {
        return end;
    }

    @JsonProperty
    public String getTopicName()
    {
        return topicName;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public List<HostAddress> getNodes()
    {
        return nodes;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return nodes;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("topicName", topicName)
                .add("partitionId", partitionId)
                .add("start", start)
                .add("end", end)
                .add("nodes", nodes)
                .toString();
    }
}
