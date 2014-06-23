package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.KafkaSplit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.LongNode;
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
import static com.google.common.base.Preconditions.checkState;

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
    public KafkaRow decodeRow(byte[] data, List<KafkaFieldDecoder<?>> fieldDecoders, List<KafkaColumnHandle> columnHandles, KafkaSplit split, long messageOffset, long messageCount)
    {
        return new KafkaJsonRow(data, fieldDecoders, columnHandles, split, messageOffset, messageCount);
    }

    public class KafkaJsonRow
            implements KafkaRow
    {
        private final JsonNode tree;
        private final JsonNode[] cache;
        private final List<KafkaFieldDecoder<?>> fieldDecoders;
        private final List<KafkaColumnHandle> columnHandles;

        KafkaJsonRow(byte[] data, List<KafkaFieldDecoder<?>> fieldDecoders, List<KafkaColumnHandle> columnHandles, KafkaSplit split, long messageOffset, long messageCount)
        {
            this.fieldDecoders = checkNotNull(fieldDecoders, "fieldDecoders is null");
            this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
            checkNotNull(split, "split is null");

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

            ImmutableSet<SpecialColumnProvider> specialColumnProviders = ImmutableSet.<SpecialColumnProvider>of(
                    new MessageColumnProvider(tree),
                    new CorruptColumnProvider(corrupted),
                    new PartitionProvider(split),
                    new SegmentStartProvider(split),
                    new SegmentEndProvider(split),
                    new MessageCountProvider(messageCount),
                    new MessageOffsetProvider(messageOffset),
                    new ByteCountProvider(data.length)
            );

            int cacheIndex = 0;
            for (KafkaColumnHandle columnHandle : columnHandles) {
                for (SpecialColumnProvider specialColumnProvider : specialColumnProviders) {
                    if (specialColumnProvider.accept(columnHandle)) {
                        cache[cacheIndex] = specialColumnProvider.value();
                        break; // for(SpecialColumnProvider ...
                    }
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
            checkState(mapping != null, "No mapping for %s", columnHandles.get(field).getColumn().getName());

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

    private static interface SpecialColumnProvider
    {
        boolean accept(KafkaColumnHandle columnHandle);

        JsonNode value();
    }

    private static class MessageColumnProvider
            implements SpecialColumnProvider
    {
        private final JsonNode tree;

        MessageColumnProvider(JsonNode tree)
        {
            this.tree = checkNotNull(tree, "tree is null");
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals(COLUMN_MESSAGE);
        }

        @Override
        public JsonNode value()
        {
            return tree;
        }
    }

    private static class CorruptColumnProvider
            implements SpecialColumnProvider
    {
        private final boolean corrupt;

        CorruptColumnProvider(boolean corrupt)
        {
            this.corrupt = corrupt;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals(COLUMN_CORRUPT);
        }

        @Override
        public JsonNode value()
        {
            return BooleanNode.valueOf(corrupt);
        }
    }

    private static class PartitionProvider
            implements SpecialColumnProvider
    {
        private final KafkaSplit split;

        PartitionProvider(KafkaSplit split)
        {
            this.split = split;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals("_partition");
        }

        @Override
        public JsonNode value()
        {
            return LongNode.valueOf(split.getPartitionId());
        }
    }

    private static class SegmentStartProvider
            implements SpecialColumnProvider
    {
        private final KafkaSplit split;

        SegmentStartProvider(KafkaSplit split)
        {
            this.split = split;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals("_start");
        }

        @Override
        public JsonNode value()
        {
            return LongNode.valueOf(split.getStart());
        }
    }

    private static class SegmentEndProvider
            implements SpecialColumnProvider
    {
        private final KafkaSplit split;

        SegmentEndProvider(KafkaSplit split)
        {
            this.split = split;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals("_end");
        }

        @Override
        public JsonNode value()
        {
            return LongNode.valueOf(split.getEnd());
        }
    }

    private static class MessageCountProvider
            implements SpecialColumnProvider
    {
        private final long messageCount;

        MessageCountProvider(long messageCount)
        {
            this.messageCount = messageCount;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals("_count");
        }

        @Override
        public JsonNode value()
        {
            return LongNode.valueOf(messageCount);
        }
    }

    private static class MessageOffsetProvider
            implements SpecialColumnProvider
    {
        private final long messageOffset;

        MessageOffsetProvider(long messageOffset)
        {
            this.messageOffset = messageOffset;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals("_offset");
        }

        @Override
        public JsonNode value()
        {
            return LongNode.valueOf(messageOffset);
        }
    }

    private static class ByteCountProvider
            implements SpecialColumnProvider
    {
        private final long byteCount;

        ByteCountProvider(long byteCount)
        {
            this.byteCount = byteCount;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getColumn().getName().equals("_bytes");
        }

        @Override
        public JsonNode value()
        {
            return LongNode.valueOf(byteCount);
        }
    }
}
