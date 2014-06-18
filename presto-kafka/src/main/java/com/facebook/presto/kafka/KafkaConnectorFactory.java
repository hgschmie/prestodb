package com.facebook.presto.kafka;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;

import javax.management.MBeanServer;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;

    KafkaConnectorFactory(TypeManager typeManager, Map<String, String> optionalConfig)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
    }

    @Override
    public String getName()
    {
        return "kafka";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new NodeModule(),
//                    new MBeanModule(),
                    new JsonModule(),
                    new KafkaModule(connectorId, typeManager),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bind(MBeanServer.class).toInstance(RebindSafeMBeanServer.wrapPlatformServer());
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
