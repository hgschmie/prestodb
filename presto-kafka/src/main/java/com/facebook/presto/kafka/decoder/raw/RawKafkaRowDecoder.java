package com.facebook.presto.kafka.decoder.raw;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RawKafkaRowDecoder
        implements KafkaRowDecoder
{
    public static final String NAME = "raw";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Set<KafkaFieldValueProvider> fieldValueProviders, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders)
    {
        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }
            @SuppressWarnings("unchecked")
            KafkaFieldDecoder<byte[]> decoder = (KafkaFieldDecoder<byte[]>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                fieldValueProviders.add(decoder.decode(data, columnHandle));
            }
        }

        return false;
    }
}
