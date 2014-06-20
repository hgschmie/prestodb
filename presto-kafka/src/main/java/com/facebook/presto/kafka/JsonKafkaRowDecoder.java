package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import org.joda.time.format.DateTimeFormat;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class JsonKafkaRowDecoder
        implements KafkaRowDecoder
{
    private static final Logger LOGGER = Logger.get(JsonKafkaRowDecoder.class);

    private final ObjectMapper objectMapper;

    @Inject
    JsonKafkaRowDecoder(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName()
    {
        return "json";
    }

    @Override
    public KafkaRow decodeRow(byte[] data, Map<String, Type> typeMap)
    {
        return new KafkaJsonRow(data, typeMap);
    }

    public class KafkaJsonRow
            implements KafkaRow
    {
        private final JsonNode tree;
        private final Map<String, JsonNode> cache = new HashMap<>();
        private final Map<String, Type> typeMap;

        KafkaJsonRow(byte[] data, Map<String, Type> typeMap)
        {
            this.typeMap = typeMap;

            JsonNode tree = null;
            try {
                tree = objectMapper.readTree(data);
                cache.put(MESSAGE_CORRUPTED, BooleanNode.FALSE);
            }
            catch (Exception e) {
                tree = MissingNode.getInstance();
                cache.put(MESSAGE_CORRUPTED, BooleanNode.TRUE);
            }

            this.tree = tree;
            cache.put(MESSAGE_WILDCARD, this.tree);
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

            if (node.isMissingNode() || node.isNull()) {
                return 0L;
            }

            Type columnType = typeMap.get(mapping);

            // Timestamp conversion hack. Remove once there is a real
            // way to convert a column into a time stamp.
            if (columnType == TimestampType.TIMESTAMP) {
                try {
                    if (node.isNumber()) {
                        return node.longValue();
                    }
                    else if (node.isTextual()) {
                        // Super hack to make twitter timestamps work. This needs to be pluggable.
                        return DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH).parseMillis(node.asText());
                    }
                    else {
                        // Coerce to long. Good luck.
                        return node.asLong();
                    }
                }
                catch (Exception e) {
                    LOGGER.warn(e, "Busted!");
                    return node.longValue();
                }
            }

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
