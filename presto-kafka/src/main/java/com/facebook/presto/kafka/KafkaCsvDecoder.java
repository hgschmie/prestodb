package com.facebook.presto.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.inject.Inject;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Maps;

import au.com.bytecode.opencsv.CSVParser;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class KafkaCsvDecoder
        implements KafkaDecoder
{
    public static final String MESSAGE_FORMAT = "csv";

    private static final CSVParser PARSER = new CSVParser();

    @Inject
    KafkaCsvDecoder()
    {
    }

    @Override
    public KafkaRow decodeRow(byte[] data, Map<String, Type> typeMap)
    {
        return new KafkaCsvRow(data);
    }

    public static class KafkaCsvRow
            implements KafkaRow
    {
        private final Map<String, String> cache = Maps.newConcurrentMap();

        KafkaCsvRow(byte [] data)
        {
            String line = new String(data, StandardCharsets.UTF_8);
            cache.put(MESSAGE_WILDCARD, line);

            try {
                String [] fields = PARSER.parseLine(line);

                for (int i = 0; i < fields.length; i++) {
                    cache.put(Integer.toString(i), fields[i]);
                }

                cache.put(KafkaDecoder.MESSAGE_CORRUPTED, "false");
            }
            catch (Exception e) {
                cache.put(KafkaDecoder.MESSAGE_CORRUPTED, "true");
            }
        }

        @Override
        public boolean getBoolean(String mapping)
        {
            String value = locateValue(mapping);
            try {
                return value != null ? Boolean.parseBoolean(value) : false;
            }
            catch (Exception e) {
                return false;
            }
        }

        @Override
        public long getLong(String mapping)
        {
            // TODO - add timestamp hack like json.
            String value = locateValue(mapping);
            try {
                return value != null ? Long.parseLong(value) : 0L;
            }
            catch (Exception e) {
                return 0L;
            }
        }

        @Override
        public double getDouble(String mapping)
        {
            String value = locateValue(mapping);
            try {
                return value != null ? Double.parseDouble(value) : 0.0d;
            }
            catch (Exception e) {
                return 0.0d;
            }
        }

        @Override
        public Slice getSlice(String mapping)
        {
            String value = locateValue(mapping);

            if (value == null) {
                return Slices.EMPTY_SLICE;
            }

            return Slices.utf8Slice(value);
        }

        @Override
        public boolean isNull(String mapping)
        {
            String value = locateValue(mapping);
            return value == null;
        }

        private String locateValue(String mapping)
        {
            if (cache.containsKey(mapping)) {
                return cache.get(mapping);
            }

            return null;
        }
    }
}
