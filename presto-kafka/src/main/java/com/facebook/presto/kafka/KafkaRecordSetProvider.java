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

import javax.inject.Inject;
import javax.inject.Named;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import io.airlift.log.Logger;

public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger LOGGER = Logger.get(KafkaRecordSetProvider.class);

    private final String connectorId;
    private final KafkaHandleResolver handleResolver;
    private final KafkaConfig kafkaConfig;

    @Inject
    public KafkaRecordSetProvider(@Named("connectorId") String connectorId,
                                  KafkaHandleResolver handleResolver,
                                  KafkaConfig kafkaConfig)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.kafkaConfig = checkNotNull(kafkaConfig, "kafkaConfig is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        LOGGER.info("getRecordSet(%s, %s)", split, columns);

        KafkaSplit kafkaSplit = handleResolver.convertSplit(split);

        ImmutableList.Builder<KafkaColumnHandle> handles = ImmutableList.builder();
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ConnectorColumnHandle handle : columns) {
            KafkaColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handles.add(columnHandle);
            types.add(columnHandle.getColumnType());
        }

        return new KafkaRecordSet(kafkaSplit, kafkaConfig, handles.build(), types.build());
    }
}
