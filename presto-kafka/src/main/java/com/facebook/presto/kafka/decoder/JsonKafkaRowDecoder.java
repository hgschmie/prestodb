package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonKafkaRowDecoder
        implements KafkaRowDecoder
{
    private static final Logger LOGGER = Logger.get(JsonKafkaRowDecoder.class);

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
    public KafkaRow decodeRow(byte[] data, List<KafkaFieldDecoder<?>> fieldDecoders, List<KafkaColumnHandle> columnHandles)
    {
        return new KafkaJsonRow(data, fieldDecoders, columnHandles);
    }

    public class KafkaJsonRow
            implements KafkaRow
    {
        private final JsonNode tree;
        private final JsonNode[] cache;
        private final List<KafkaFieldDecoder<?>> fieldDecoders;
        private final List<KafkaColumnHandle> columnHandles;

        KafkaJsonRow(byte[] data, List<KafkaFieldDecoder<?>> fieldDecoders, List<KafkaColumnHandle> columnHandles)
        {
            this.fieldDecoders = checkNotNull(fieldDecoders, "fieldDecoders is null");
            this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

            JsonNode tree = null;
            cache = new JsonNode[columnHandles.size()];

            boolean corrupted = false;
            try {
                tree = objectMapper.readTree(data);
            }
            catch (Exception e) {
                tree = MissingNode.getInstance();
                corrupted = true;
            }

            this.tree = tree;

            int cacheIndex = 0;
            for (KafkaColumnHandle columnHandle : columnHandles) {
                // add the full line as _message
                if (columnHandle.getColumn().getName().equals(COLUMN_MESSAGE)) {
                    cache[cacheIndex] = tree;
                }
                // add the corrupt / not corrupt flag as _corrupt
                else if (columnHandle.getColumn().getName().equals(COLUMN_CORRUPT)) {
                    cache[cacheIndex] = BooleanNode.valueOf(corrupted);
                }
                cacheIndex++;
            }
        }

        @Override
        public boolean getBoolean(int field)
        {
            KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeBoolean(locateNode(field), columnHandle.getColumn().getFormat());
        }

        @Override
        public long getLong(int field)
        {
            KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeLong(locateNode(field), columnHandle.getColumn().getFormat());
        }

        @Override
        public double getDouble(int field)
        {
            KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeDouble(locateNode(field), columnHandle.getColumn().getFormat());
        }

        @Override
        public Slice getSlice(int field)
        {
            KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeSlice(locateNode(field), columnHandle.getColumn().getFormat());
        }

        @Override
        public boolean isNull(int field)
        {
            KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.isNull(locateNode(field), columnHandle.getColumn().getFormat());
        }

        private JsonNode locateNode(int field)
        {
            if (cache[field] != null) {
                return cache[field];
            }

            String mapping = columnHandles.get(field).getColumn().getMapping();

            JsonNode currentNode = tree;
            for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
                if (!currentNode.has(pathElement)) {
                    currentNode = MissingNode.getInstance();
                    break;
                }
                currentNode = currentNode.path(pathElement);
            }
            cache[field] = currentNode;
            return currentNode;
        }
    }

    public static class KafkaJsonFieldDecoder
            implements KafkaFieldDecoder<JsonNode>
    {
        @Override
        public Set<Class<?>> getJavaTypes()
        {
            return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
        }

        @Override
        public final String getRowDecoderName()
        {
            return NAME;
        }

        @Override
        public String getFieldDecoderName()
        {
            return KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
        }

        @Override
        public boolean decodeBoolean(JsonNode value, String format)
        {
            checkNotNull(value, "value is null");
            return value.asBoolean();
        }

        @Override
        public long decodeLong(JsonNode value, String format)
        {
            checkNotNull(value, "value is null");
            return value.asLong();
        }

        @Override
        public double decodeDouble(JsonNode value, String format)
        {
            checkNotNull(value, "value is null");
            return value.asDouble();
        }

        @Override
        public Slice decodeSlice(JsonNode value, String format)
        {
            checkNotNull(value, "value is null");
            return isNull(value, format) ? Slices.EMPTY_SLICE : Slices.utf8Slice(value.isValueNode() ? value.asText() : value.toString());
        }

        @Override
        public boolean isNull(JsonNode value, String format)
        {
            checkNotNull(value, "value is null");
            return value.isMissingNode() || value.isNull();
        }
    }
}
