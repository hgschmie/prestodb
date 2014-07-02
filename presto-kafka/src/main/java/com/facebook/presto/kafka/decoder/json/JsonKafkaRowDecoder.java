package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * JSON specific Kafka row decoder.
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
    public boolean decodeRow(byte[] data, Set<KafkaFieldValueProvider> fieldValueProviders, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders)
    {
        JsonNode tree;

        try {
            tree = objectMapper.readTree(data);
        }
        catch (Exception e) {
            return true;
        }

        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }
            @SuppressWarnings("unchecked")
            KafkaFieldDecoder<JsonNode> decoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                JsonNode node = locateNode(tree, columnHandle);
                fieldValueProviders.add(decoder.decode(node, columnHandle));
            }
        }

        return false;
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
