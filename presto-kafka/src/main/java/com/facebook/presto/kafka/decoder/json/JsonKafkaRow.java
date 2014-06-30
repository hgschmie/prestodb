package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldValueProvider;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class JsonKafkaRow
        extends KafkaRow
{
    private final JsonNode tree;
    private final JsonNode[] cache;
    private final Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders;

    JsonKafkaRow(JsonNode tree, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders, Set<KafkaInternalFieldValueProvider> internalFieldValueProviders)
    {
        super(columnHandles, internalFieldValueProviders);

        this.fieldDecoders = checkNotNull(fieldDecoders, "fieldDecoders is null");

        this.tree = tree;
        cache = new JsonNode[columnHandles.size()];
    }

    @Override
    protected boolean getBoolean(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeBoolean(locateNode(columnHandle, field), columnHandle.getFormatHint());
    }

    @Override
    protected long getLong(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeLong(locateNode(columnHandle, field), columnHandle.getFormatHint());
    }

    @Override
    protected double getDouble(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeDouble(locateNode(columnHandle, field), columnHandle.getFormatHint());
    }

    @Override
    protected Slice getSlice(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeSlice(locateNode(columnHandle, field), columnHandle.getFormatHint());
    }

    @Override
    protected boolean isNull(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<JsonNode> fieldDecoder = (KafkaFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);
        return fieldDecoder.isNull(locateNode(columnHandle, field), columnHandle.getFormatHint());
    }

    private JsonNode locateNode(KafkaColumnHandle columnHandle, int field)
    {
        if (cache[field] != null) {
            return cache[field];
        }

        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

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
