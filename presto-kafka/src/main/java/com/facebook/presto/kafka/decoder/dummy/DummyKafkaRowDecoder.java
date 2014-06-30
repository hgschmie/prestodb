package com.facebook.presto.kafka.decoder.dummy;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldValueProvider;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DummyKafkaRowDecoder
        implements KafkaRowDecoder
{
    public static final String NAME = "dummy";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public KafkaRow decodeRow(byte[] data, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders, Set<KafkaInternalFieldValueProvider> internalFieldValueProviders)
    {
        return new DummyKafkaRow(columnHandles, internalFieldValueProviders);
    }
}
