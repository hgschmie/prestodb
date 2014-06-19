package com.facebook.presto.kafka;

import io.airlift.slice.Slice;

public interface KafkaRow
{
    boolean getBoolean(String mapping);

    long getLong(String mapping);

    double getDouble(String mapping);

    Slice getSlice(String mapping);

    boolean isNull(String mapping);
}
