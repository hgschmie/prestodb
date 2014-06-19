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

import static java.lang.String.format;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.airlift.log.Logger;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOGGER = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaConfig kafkaConfig;
    private final KafkaHandleResolver handleResolver;

    @Inject
    public KafkaSplitManager(@Named("connectorId") String connectorId,
                             KafkaConfig kafkaConfig,
                             KafkaHandleResolver handleResolver)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.kafkaConfig = checkNotNull(kafkaConfig, "kafkaConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        LOGGER.info("getPartitions(%s, %s)", tableHandle, tupleDomain);

        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        try (CloseableSimpleConsumer simpleConsumer = getSimpleConsumer()) {
            TopicMetadataRequest req = new TopicMetadataRequest(ImmutableList.of(kafkaTableHandle.getTopicName()));
            TopicMetadataResponse resp = simpleConsumer.send(req);

            ImmutableList.Builder<ConnectorPartition> builder = ImmutableList.builder();

            for (TopicMetadata metadata : resp.topicsMetadata()) {
                for (PartitionMetadata part : metadata.partitionsMetadata()) {
                    LOGGER.debug("Adding Partition %s/%s", metadata.topic(), part.partitionId());
                    Broker leader = part.leader();
                    if (leader == null) { // Leader election going on...
                        LOGGER.warn("No leader for partition %s/%s found!", metadata.topic(), part.partitionId());
                    }
                    else {
                        builder.add(new KafkaPartition(metadata.topic(),
                                                       part.partitionId(),
                                                       HostAddress.fromParts(leader.host(), leader.port()),
                                                       // TODO - that may probably be isr().
                                                       ImmutableList.copyOf(Lists.transform(part.replicas(), getBrokerToHostAddressFunction()))));
                    }
                }
            }

            return new ConnectorPartitionResult(builder.build(), tupleDomain);
        }
        catch (Exception e) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName(), e);
        }
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        LOGGER.info("getPartitionSplits(%s, %s)", tableHandle, partitions);

        handleResolver.convertTableHandle(tableHandle);

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        for (ConnectorPartition cp : partitions) {
            checkState(cp instanceof KafkaPartition, "Found an unknown partition type: %s", cp.getClass().getSimpleName());
            KafkaPartition partition = (KafkaPartition) cp;

            LOGGER.info("Processing Partition %s", partition);

            try (CloseableSimpleConsumer leaderConsumer = getSimpleConsumer(partition.getPartitionLeader())) {
                // Kafka contains a reverse list of "end - start" pairs for the splits
                long [] endOffsets = findOffsets(leaderConsumer, partition, kafka.api.OffsetRequest.LatestTime());
                LOGGER.info("End Offsets are: %s", Arrays.toString(endOffsets));

                for (int i = endOffsets.length - 1; i > 0; i--) {
                    KafkaSplit split = new KafkaSplit(connectorId, endOffsets[i], endOffsets[i - 1], partition.getPartitionNodes());
                    LOGGER.info("Adding Split: %s", split);
                    builder.add(split);
                }
            }
        }

        return new FixedSplitSource(connectorId, builder.build());
    }

    private long [] findOffsets(SimpleConsumer consumer, KafkaPartition partition, long time)
    {
        TopicAndPartition tap = new TopicAndPartition(partition.getTopicName(), partition.getPartitionIdAsInt());
        PartitionOffsetRequestInfo pori = new PartitionOffsetRequestInfo(time, Integer.MAX_VALUE); // TODO - Is there a constant for "get all of them?"
        OffsetRequest or = new OffsetRequest(ImmutableMap.of(tap, pori), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse oresp = consumer.getOffsetsBefore(or);
        return oresp.offsets(partition.getTopicName(), partition.getPartitionIdAsInt());
    }

    private CloseableSimpleConsumer getSimpleConsumer()
    {
        List<HostAddress> nodes = new ArrayList<>(kafkaConfig.getNodes());
        Collections.shuffle(nodes);

        return getSimpleConsumer(nodes.get(0));
    }

    private CloseableSimpleConsumer getSimpleConsumer(HostAddress host)
    {
        return new CloseableSimpleConsumer(host.getHostText(),
                                           host.getPort(),
                                           kafkaConfig.getKafkaConnectTimeout().toMillis(),
                                           kafkaConfig.getKafkaBufferSize().toBytes(),
                                           format("presto-kafka-%s-split-manager", connectorId));
    }

    private Function<Broker, HostAddress> getBrokerToHostAddressFunction()
    {
        return new Function<Broker, HostAddress>() {
            @Override
            public HostAddress apply(@Nonnull Broker broker)
            {
                return HostAddress.fromParts(broker.host(), broker.port());
            }
        };
    }
}
