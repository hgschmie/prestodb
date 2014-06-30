package com.facebook.presto.kafka.decoder;

import io.airlift.slice.Slice;

import java.util.Set;

public interface KafkaFieldDecoder<T>
{
    String DEFAULT_FIELD_DECODER_NAME = "_default";

    public Set<Class<?>> getJavaTypes();

    public String getRowDecoderName();

    public String getFieldDecoderName();

    boolean decodeBoolean(T value, String formatHint);

    long decodeLong(T value, String formatHint);

    double decodeDouble(T value, String formatHint);

    Slice decodeSlice(T value, String formatHint);

    boolean isNull(T value, String formatHint);
}
