package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.decoder.ConstantBooleanProvider;
import com.facebook.presto.kafka.decoder.InternalColumnProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonKafkaRowDecoder
        implements KafkaRowDecoder
{
    private static final Logger LOG = Logger.get(JsonKafkaRowDecoder.class);

    public static final String NAME = "json";

    private final ObjectMapper objectMapper;

    @Inject
    JsonKafkaRowDecoder(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public KafkaRow decodeRow(byte[] data, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders, Set<InternalColumnProvider> internalColumnProviders)
    {
        JsonNode tree;
        boolean corrupted = false;

        try {
            tree = objectMapper.readTree(data);
        }
        catch (Exception e) {
            tree = MissingNode.getInstance();
            corrupted = true;
        }

        Set<InternalColumnProvider> allInternalColumnProviders = ImmutableSet.<InternalColumnProvider>builder()
                .addAll(internalColumnProviders)
                .add(new ConstantBooleanProvider("_corrupted", corrupted))
                .build();

        return new JsonKafkaRow(tree, columnHandles, fieldDecoders, allInternalColumnProviders);
    }
}
