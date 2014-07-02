package com.facebook.presto.kafka.decoder.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;

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
    public boolean decodeRow(byte[] data, Set<KafkaFieldValueProvider> fieldValueProviders, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders)
    {
        String[] fields;
        try {
            String line = new String(data, StandardCharsets.UTF_8);
            fields = parser.parseLine(line);
        }
        catch (Exception e) {
            return true;
        }

        int index = 0;
        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (index >= fields.length) {
                break;
            }
            if (columnHandle.isInternal()) {
                continue;
            }

            @SuppressWarnings("unchecked")
            KafkaFieldDecoder<String> decoder = (KafkaFieldDecoder<String>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                fieldValueProviders.add(decoder.decode(fields[index], columnHandle));
            }
            index++;
        }
        return false;
    }
}
