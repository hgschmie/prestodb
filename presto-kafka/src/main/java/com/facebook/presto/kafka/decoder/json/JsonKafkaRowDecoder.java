package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.KafkaInternalFieldDescription;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * JSON specific Kafka row decoder. If a topic message can not be parsed, set the {@link com.facebook.presto.kafka.KafkaInternalFieldDescription#CORRUPT_FIELD} to true.
 */
public class JsonKafkaRowDecoder
        implements KafkaRowDecoder
{
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
    public Set<KafkaFieldValueProvider> decodeRow(byte[] data, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders)
    {
        boolean corrupted = false;
        JsonNode tree;

        ImmutableSet.Builder<KafkaFieldValueProvider> builder = ImmutableSet.builder();

        try {
            tree = objectMapper.readTree(data);
        }
        catch (Exception e) {
            tree = MissingNode.getInstance();
            corrupted = true;
        }

        builder.add(KafkaInternalFieldDescription.CORRUPT_FIELD.forBooleanValue(corrupted));

        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }
            KafkaFieldDecoder<JsonNode> decoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);
            if (decoder != null) {
                JsonNode node = locateNode(tree, columnHandle);
                builder.add(decoder.decode(node, columnHandle));
            }
        }

        return builder.build();
    }

    private JsonNode locateNode(JsonNode tree, KafkaColumnHandle columnHandle)
    {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }
}
