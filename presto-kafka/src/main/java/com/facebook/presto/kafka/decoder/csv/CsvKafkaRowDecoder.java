package com.facebook.presto.kafka.decoder.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.decoder.ConstantBooleanProvider;
import com.facebook.presto.kafka.decoder.InternalColumnProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CsvKafkaRowDecoder
        implements KafkaRowDecoder
{
    public static final String NAME = "csv";

    private static final CSVParser PARSER = new CSVParser();

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
    public KafkaRow decodeRow(byte[] data, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders, Set<InternalColumnProvider> internalColumnProviders)
    {
        boolean corrupted = false;
        String[] fields = null;

        try {
            String line = new String(data, StandardCharsets.UTF_8);
            fields = PARSER.parseLine(line);
        }
        catch (Exception e) {
            corrupted = true;
        }

        Set<InternalColumnProvider> allInternalColumnProviders = ImmutableSet.<InternalColumnProvider>builder()
                .addAll(internalColumnProviders)
                .add(new ConstantBooleanProvider("_corrupted", corrupted))
                .build();

        return new CsvKafkaRow(fields, columnHandles, fieldDecoders, allInternalColumnProviders);
    }
}
