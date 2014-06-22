package com.facebook.presto.kafka.decoder;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.kafka.decoder.KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class KafkaDecoderRegistry
{
    private static final Logger LOG = Logger.get(KafkaDecoderRegistry.class);

    private final Map<String, KafkaRowDecoder> rowDecoders;
    private final Map<String, SetMultimap<Class<?>, KafkaFieldDecoder<?>>> fieldDecoders;

    @Inject
    KafkaDecoderRegistry(Set<KafkaRowDecoder> rowDecoders,
            Set<KafkaFieldDecoder> fieldDecoders)
    {
        checkNotNull(rowDecoders, "rowDecoders is null");

        ImmutableMap.Builder<String, KafkaRowDecoder> rowBuilder = ImmutableMap.builder();
        for (KafkaRowDecoder decoder : rowDecoders) {
            rowBuilder.put(decoder.getName(), decoder);
        }

        this.rowDecoders = rowBuilder.build();

        Map<String, ImmutableSetMultimap.Builder<Class<?>, KafkaFieldDecoder<?>>> fieldDecoderBuilders = new HashMap<>();

        for (KafkaFieldDecoder<?> fieldDecoder : fieldDecoders) {
            ImmutableSetMultimap.Builder<Class<?>, KafkaFieldDecoder<?>> fieldDecoderBuilder = fieldDecoderBuilders.get(fieldDecoder.getRowDecoderName());
            if (fieldDecoderBuilder == null) {
                fieldDecoderBuilder = ImmutableSetMultimap.builder();
                fieldDecoderBuilders.put(fieldDecoder.getRowDecoderName(), fieldDecoderBuilder);
            }

            for (Class<?> clazz : fieldDecoder.getJavaTypes()) {
                fieldDecoderBuilder.put(clazz, fieldDecoder);
            }
        }

        ImmutableMap.Builder<String, SetMultimap<Class<?>, KafkaFieldDecoder<?>>> fieldDecoderBuilder = ImmutableMap.builder();
        for (Map.Entry<String, ImmutableSetMultimap.Builder<Class<?>, KafkaFieldDecoder<?>>> entry : fieldDecoderBuilders.entrySet()) {
            fieldDecoderBuilder.put(entry.getKey(), entry.getValue().build());
        }

        this.fieldDecoders = fieldDecoderBuilder.build();
        LOG.info("All field decoders found: %s", this.fieldDecoders);
    }

    public KafkaRowDecoder getRowDecoder(String decoderName)
    {
        checkState(rowDecoders.containsKey(decoderName), "no row decoder for '%s' found", decoderName);
        return rowDecoders.get(decoderName);
    }

    public KafkaFieldDecoder<?> getFieldDecoder(String rowDecoderName, Class<?> fieldType, @Nullable String fieldDecoderName)
    {
        checkNotNull(rowDecoderName, "rowDecoderName is null");
        checkNotNull(fieldType, "fieldType is null");

        checkState(fieldDecoders.containsKey(rowDecoderName), "no field decoders for java type '%s' found", rowDecoderName);
        Set<KafkaFieldDecoder<?>> decoders = fieldDecoders.get(rowDecoderName).get(fieldType);

        ImmutableSet<String> fieldNames = ImmutableSet.of(Objects.firstNonNull(fieldDecoderName, DEFAULT_FIELD_DECODER_NAME),
                DEFAULT_FIELD_DECODER_NAME);

        for (String fieldName : fieldNames) {
            for (KafkaFieldDecoder decoder : decoders) {
                if (fieldName.equals(decoder.getFieldDecoderName())) {
                    return decoder;
                }
            }
        }

        throw new IllegalStateException(format("No decoder for %s/%s found!", rowDecoderName, fieldType));
    }
}
