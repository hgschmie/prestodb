package com.facebook.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import kafka.javaapi.consumer.SimpleConsumer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class KafkaSimpleConsumerManager
{
    private static final Logger LOG = Logger.get(KafkaSimpleConsumerManager.class);

    private final LoadingCache<HostAddress, SimpleConsumer> consumerCache;

    private final String connectorId;
    private final KafkaConfig kafkaConfig;
    private final NodeManager nodeManager;

    @Inject
    KafkaSimpleConsumerManager(@Named("connectorId") String connectorId,
            KafkaConfig kafkaConfig,
            NodeManager nodeManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.kafkaConfig = checkNotNull(kafkaConfig, "kafkaConfig is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");

        this.consumerCache = CacheBuilder.newBuilder().build(new SimpleConsumerCacheLoader());
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<HostAddress, SimpleConsumer> entry : consumerCache.asMap().entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (Exception e) {
                LOG.warn(e, "While closing consumer %s:", entry.getKey());
            }
        }
    }

    public SimpleConsumer getConsumer(HostAddress host)
    {
        checkNotNull(host, "host is null");
        try {
            return consumerCache.get(host);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class SimpleConsumerCacheLoader
            extends CacheLoader<HostAddress, SimpleConsumer>
    {
        @Override
        public SimpleConsumer load(HostAddress host)
                throws Exception
        {
            LOG.info("Creating new Consumer for %s", host);
            return new SimpleConsumer(host.getHostText(),
                    host.getPort(),
                    Ints.checkedCast(kafkaConfig.getKafkaConnectTimeout().toMillis()),
                    Ints.checkedCast(kafkaConfig.getKafkaBufferSize().toBytes()),
                    format("presto-kafka-%s-%s", connectorId, nodeManager.getCurrentNode().getNodeIdentifier()));
        }
    }
}
