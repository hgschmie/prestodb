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
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

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

    private final KafkaHandleResolver handleResolver;
    private final KafkaSimpleConsumerManager consumerManager;
    private final Map<String, KafkaDecoder> decoders;

    @Inject
    public KafkaRecordSetProvider(Map<String, KafkaDecoder> decoders,
                                  KafkaHandleResolver handleResolver,
                                  KafkaSimpleConsumerManager consumerManager)
    {
        this.decoders = checkNotNull(decoders, "decoders is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.consumerManager = checkNotNull(consumerManager, "consumerManager is null");

        LOGGER.info("Found %s as decoders", decoders);
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        KafkaSplit kafkaSplit = handleResolver.convertSplit(split);

        String decoderType = kafkaSplit.getDecoderType();

        LOGGER.info("Decoder in use: %s (%s)", decoders.get(decoderType), decoderType);

        checkState(decoders.containsKey(decoderType), "no decoder for type '%s' found", decoderType);

        ImmutableList.Builder<KafkaColumnHandle> handles = ImmutableList.builder();
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ConnectorColumnHandle handle : columns) {
            KafkaColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handles.add(columnHandle);
            types.add(columnHandle.getColumnType());
        }

        return new KafkaRecordSet(decoders.get(decoderType), kafkaSplit, consumerManager, handles.build(), types.build());
    }
}
