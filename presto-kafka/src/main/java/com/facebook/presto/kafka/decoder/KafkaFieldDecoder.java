package com.facebook.presto.kafka.decoder;

import io.airlift.slice.Slice;

import java.util.Set;

public interface KafkaFieldDecoder<T>
{
    String DEFAULT_FIELD_DECODER_NAME = "_default";

    public Set<Class<?>> getJavaTypes();

    public String getRowDecoderName();

    public String getFieldDecoderName();

    boolean decodeBoolean(T value, String format);

    long decodeLong(T value, String format);

    double decodeDouble(T value, String format);

    Slice decodeSlice(T value, String format);

    boolean isNull(T value, String format);
}
