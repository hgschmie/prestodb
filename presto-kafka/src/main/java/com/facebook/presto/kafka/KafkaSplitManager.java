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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;

    @Inject
    public KafkaSplitManager(@Named("connectorId") String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkArgument(tableHandle instanceof KafkaTableHandle, "tableHandle is not an instance of KafkaTableHandle");
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) tableHandle;
        return new ConnectorPartitionResult(ImmutableList.<ConnectorPartition>of(), tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        throw new UnsupportedOperationException();
    }
}
