package com.facebook.presto.kafka;

import com.facebook.presto.kafka.decoder.KafkaDecoderModule;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

/**
 * Guice module for the Apache Kafka connector.
 */
public class KafkaConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(KafkaConnector.class).in(Scopes.SINGLETON);

        binder.bind(KafkaHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(KafkaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KafkaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KafkaRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(KafkaSimpleConsumerManager.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(KafkaConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(KafkaTopicDescription.class);

        binder.install(new KafkaDecoderModule());

        for (KafkaInternalFieldDescription internalFieldDescription : KafkaInternalFieldDescription.getInternalFields()) {
            bindInternalColumn(binder, internalFieldDescription);
        }
    }

    private static void bindInternalColumn(Binder binder, KafkaInternalFieldDescription fieldDescription)
    {
        Multibinder<KafkaInternalFieldDescription> fieldDescriptionBinder = Multibinder.newSetBinder(binder, KafkaInternalFieldDescription.class);
        fieldDescriptionBinder.addBinding().toInstance(fieldDescription);
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
