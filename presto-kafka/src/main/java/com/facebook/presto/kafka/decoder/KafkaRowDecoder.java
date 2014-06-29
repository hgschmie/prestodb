package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KafkaRowDecoder
{
    String getName();

    KafkaRow decodeRow(byte[] data,
            List<KafkaColumnHandle> columnHandles,
            Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders,
            Set<InternalColumnProvider> internalColumnProviders);
}
