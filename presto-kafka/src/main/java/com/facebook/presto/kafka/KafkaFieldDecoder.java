package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.Type;

import io.airlift.slice.Slice;

public interface KafkaFieldDecoder
{
    public Type getType();

    public String getColumnDecoderName();

    boolean getBoolean();

    long getLong();

    double getDouble();

    Slice getSlice();

    boolean isNull();
}
