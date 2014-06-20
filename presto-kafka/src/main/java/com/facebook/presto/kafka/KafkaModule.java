package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

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
        binder.bind(KafkaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KafkaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KafkaRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(KafkaSimpleConsumerManager.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(KafkaConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(KafkaTable.class);

        MapBinder<String, KafkaDecoder> decoderBinder = MapBinder.newMapBinder(binder, String.class, KafkaDecoder.class);
        decoderBinder.addBinding(KafkaJsonDecoder.MESSAGE_FORMAT).to(KafkaJsonDecoder.class).in(Scopes.SINGLETON);
        decoderBinder.addBinding(KafkaCsvDecoder.MESSAGE_FORMAT).to(KafkaCsvDecoder.class).in(Scopes.SINGLETON);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(value);
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
