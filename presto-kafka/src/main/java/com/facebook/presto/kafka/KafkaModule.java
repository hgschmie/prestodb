package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;

public class KafkaModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    KafkaModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bindConstant().annotatedWith(Names.named("connectorId")).to(connectorId);

        binder.bind(KafkaConnector.class).in(Scopes.SINGLETON);

        binder.bind(KafkaHandleResolver.class).in(Scopes.SINGLETON);

//        binder.bind(KafkaMetadata.class).in(Scopes.SINGLETON);
//        binder.bind(KafkaSplitManager.class).in(Scopes.SINGLETON);
//        binder.bind(KafkaRecordSetProvider.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(KafkaConfig.class);
    }
}
