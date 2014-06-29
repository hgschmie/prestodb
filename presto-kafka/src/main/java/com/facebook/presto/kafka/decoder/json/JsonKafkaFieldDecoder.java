package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonKafkaFieldDecoder
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
        return JsonKafkaRowDecoder.NAME;
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
