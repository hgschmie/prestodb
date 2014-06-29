package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import io.airlift.slice.Slice;

public interface InternalColumnProvider
{
    boolean accept(KafkaColumnHandle columnHandle);

    boolean getBoolean();

    long getLong();

    double getDouble();

    Slice getSlice();

    boolean isNull();
}
