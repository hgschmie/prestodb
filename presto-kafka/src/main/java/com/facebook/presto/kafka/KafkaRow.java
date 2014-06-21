package com.facebook.presto.kafka;

import io.airlift.slice.Slice;

public interface KafkaRow

{
    boolean getBoolean(int field);

    long getLong(int field);

    double getDouble(int field);

    Slice getSlice(int field);

    boolean isNull(int field);
}
