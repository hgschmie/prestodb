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
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;

    @Inject
    public KafkaRecordSetProvider(@Named("connectorId") String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        throw new UnsupportedOperationException();

        // checkNotNull(split, "partitionChunk is null");
        // checkArgument(split instanceof KafkaSplit);

        // KafkaSplit kafkaSplit = (KafkaSplit) split;
        // checkArgument(kafkaSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        // ImmutableList.Builder<KafkaColumnHandle> handles = ImmutableList.builder();
        // for (ConnectorColumnHandle handle : columns) {
        //     checkArgument(handle instanceof KafkaColumnHandle);
        //     handles.add((KafkaColumnHandle) handle);
        // }

        // return new KafkaRecordSet(kafkaSplit, handles.build());
    }
}
