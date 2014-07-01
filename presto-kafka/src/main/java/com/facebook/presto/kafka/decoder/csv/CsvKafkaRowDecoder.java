package com.facebook.presto.kafka.decoder.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldDescription;
import com.facebook.presto.kafka.KafkaInternalFieldValueProvider;
import com.facebook.presto.kafka.KafkaRow;
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
    public KafkaRow decodeRow(byte[] data, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders, Set<KafkaInternalFieldValueProvider> internalFieldValueProviders)
    {
        boolean corrupted = false;
        String[] fields = EMPTY_FIELDS;

        try {
            String line = new String(data, StandardCharsets.UTF_8);
            fields = parser.parseLine(line);
        }
        catch (Exception e) {
            corrupted = true;
        }

        Set<KafkaInternalFieldValueProvider> rowInternalFieldValueProviders = ImmutableSet.<KafkaInternalFieldValueProvider>builder()
                .addAll(internalFieldValueProviders)
                .add(KafkaInternalFieldDescription.CORRUPT_FIELD.forBooleanValue(corrupted))
                .build();

        return new CsvKafkaRow(fields, columnHandles, fieldDecoders, rowInternalFieldValueProviders);
    }
}
