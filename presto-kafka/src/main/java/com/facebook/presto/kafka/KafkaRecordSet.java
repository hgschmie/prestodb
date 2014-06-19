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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;

import io.airlift.log.Logger;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;

public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger LOGGER = Logger.get(KafkaRecordSet.class);

    private final KafkaConfig kafkaConfig;
    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final FetchResponse fetchResponse;
    private final KafkaSplit split;

    KafkaRecordSet(KafkaSplit split, KafkaConfig kafkaConfig, List<KafkaColumnHandle> columnHandles, List<Type> columnTypes)
    {
        this.split = checkNotNull(split, "split is null");

        this.kafkaConfig = checkNotNull(kafkaConfig, "kafkaConfig is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
        this.columnTypes = checkNotNull(columnTypes, "types is null");

        FetchRequest req = new FetchRequestBuilder()
            .clientId("presto-worker-" + UUID.randomUUID().toString())
            .addFetch(split.getTopicName(), split.getPartitionId(), split.getStart(), 1_000_000)
            .build();

        try (CloseableSimpleConsumer consumer = getSimpleConsumer(split.getNodes())) {
            LOGGER.info("Fetching data for %s", split);
            fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                LOGGER.warn("Fetch response has error: %s", fetchResponse.errorCode(split.getTopicName(), split.getPartitionId()));
                throw new IllegalStateException("Kafka split error!");
            }
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        LOGGER.info("cursor()");
        return new KafkaRecordCursor(split, columnHandles, fetchResponse);
    }

    private CloseableSimpleConsumer getSimpleConsumer(Collection<HostAddress> addresses)
    {
        // TODO - this should look at the actual node this is running on and prefer
        // that copy if running locally. - look into NodeInfo
        List<HostAddress> nodes = new ArrayList<>(addresses);
        Collections.shuffle(nodes);

        return getSimpleConsumer(nodes.get(0));
    }

    private CloseableSimpleConsumer getSimpleConsumer(HostAddress host)
    {
        return new CloseableSimpleConsumer(host.getHostText(),
                                           host.getPort(),
                                           kafkaConfig.getKafkaConnectTimeout().toMillis(),
                                           kafkaConfig.getKafkaBufferSize().toBytes(),
                                           "presto-worker-" + UUID.randomUUID().toString());
    }
}
