package com.facebook.presto.kafka;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final Map<String, String> optionalConfig;

    KafkaConnectorFactory(TypeManager typeManager,
            NodeManager nodeManager,
            Map<String, String> optionalConfig)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
    }

    @Override
    public String getName()
    {
        return "kafka";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> config)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new KafkaModule(),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bindConstant().annotatedWith(Names.named("connectorId")).to(connectorId);
                            binder.bind(TypeManager.class).toInstance(typeManager);
                            binder.bind(NodeManager.class).toInstance(nodeManager);
                        }
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(KafkaConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
