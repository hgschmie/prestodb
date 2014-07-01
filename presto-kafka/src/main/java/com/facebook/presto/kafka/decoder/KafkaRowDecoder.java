package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldValueProvider;
import com.facebook.presto.kafka.KafkaRow;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decodes a Kafka message from bytes into a decoder specific internal representation.
 */
public interface KafkaRowDecoder
{
    /**
     * Returns the row decoder specific name. This name will be selected with the {@link com.facebook.presto.kafka.KafkaTopicDescription#dataFormat} value.
     */
    String getName();

    KafkaRow decodeRow(byte[] data,
            List<KafkaColumnHandle> columnHandles,
            Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders,
            Set<KafkaInternalFieldValueProvider> internalFieldValueProviders);
}
