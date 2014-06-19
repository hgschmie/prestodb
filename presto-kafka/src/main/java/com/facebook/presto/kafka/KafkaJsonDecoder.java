package com.facebook.presto.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.util.Map;

public class KafkaJsonDecoder
        implements KafkaDecoder
{
    private final ObjectMapper objectMapper;

    public static final String MESSAGE_FORMAT = "json";
    public static final String MESSAGE_WILDCARD = "*";

    @Inject
    public KafkaJsonDecoder(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public KafkaRow decodeRow(byte[] data)
    {
        try {
            return new KafkaJsonRow(objectMapper.readTree(data));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static class KafkaJsonRow
            implements KafkaRow
    {
        private final JsonNode tree;
        private final Map<String, JsonNode> cache = Maps.newConcurrentMap();

        KafkaJsonRow(JsonNode tree)
        {
            this.tree = tree;
            cache.put(MESSAGE_WILDCARD, tree);
        }

        @Override
        public boolean getBoolean(String mapping)
        {
            JsonNode node = locateNode(mapping);
            return node.booleanValue();
        }

        @Override
        public long getLong(String mapping)
        {
            JsonNode node = locateNode(mapping);
            return node.longValue();
        }

        @Override
        public double getDouble(String mapping)
        {
            JsonNode node = locateNode(mapping);
            return node.doubleValue();
        }

        @Override
        public Slice getSlice(String mapping)
        {
            JsonNode node = locateNode(mapping);

            if (node.isNull() || node.isMissingNode()) {
                return Slices.EMPTY_SLICE;
            }

            return Slices.utf8Slice(node.isTextual() ? node.asText() : node.toString());
        }

        @Override
        public boolean isNull(String mapping)
        {
            JsonNode node = locateNode(mapping);
            return node.isMissingNode() || node.isNull();
        }

        private JsonNode locateNode(String mapping)
        {
            if (cache.containsKey(mapping)) {
                return cache.get(mapping);
            }

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
}
