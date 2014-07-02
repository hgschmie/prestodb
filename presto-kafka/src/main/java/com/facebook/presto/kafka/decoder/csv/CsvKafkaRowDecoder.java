package com.facebook.presto.kafka.decoder.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldDescription;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decode a Kafka message as CSV. This is an extremely primitive CSV decoder using {@link au.com.bytecode.opencsv.CSVParser]}.
 * <p/>
 * It needs some work to be really useful but may work for simple, evenly structured topics.
 */
public class CsvKafkaRowDecoder
        implements KafkaRowDecoder
{
    public static final String NAME = "csv";

    private static final String[] EMPTY_FIELDS = new String[0];

    private final CSVParser parser = new CSVParser();

    @Inject
    CsvKafkaRowDecoder()
    {
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Set<KafkaFieldValueProvider> decodeRow(byte[] data, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders)
    {
        boolean corrupted = false;
        String[] fields = EMPTY_FIELDS;

        ImmutableSet.Builder<KafkaFieldValueProvider> builder = ImmutableSet.builder();

        try {
            String line = new String(data, StandardCharsets.UTF_8);
            fields = parser.parseLine(line);
        }
        catch (Exception e) {
            corrupted = true;
        }

        builder.add(KafkaInternalFieldDescription.CORRUPT_FIELD.forBooleanValue(corrupted));

        int index = 0;
        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (index >= fields.length) {
                break;
            }
            if (columnHandle.isInternal()) {
                continue;
            }
            KafkaFieldDecoder<String> decoder = (KafkaFieldDecoder<String>) fieldDecoders.get(columnHandle);
            if (decoder != null) {
                builder.add(decoder.decode(fields[index++], columnHandle));
            }
        }

        return builder.build();
    }
}
