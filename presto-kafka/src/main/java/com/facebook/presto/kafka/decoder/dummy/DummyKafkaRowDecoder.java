package com.facebook.presto.kafka.decoder.dummy;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldValueProvider;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The row decoder for the 'dummy' format. As Kafka is an unstructured message format (bag'o'bytes), a specific decoder for a topic must exist. To start developing such a decoder,
 * it is beneficial to be able to configure an arbitrary topic to be available through presto without any decoding at all (not even line parsing) and examine the internal rows
 * that are exposed by Presto (see {@link com.facebook.presto.kafka.KafkaInternalFieldDescription} for a list of internal columns that are available to each configured topic).
 * By adding a topic name to the catalog configuration file and not having and specific topic description JSON file (or by omitting the 'dataFormat' field in the topic description file),
 * this decoder is selected, which intentionally does not do *anything* with the messages read from Kafka.
 */
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
