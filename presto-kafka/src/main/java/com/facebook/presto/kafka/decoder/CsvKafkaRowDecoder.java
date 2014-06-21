package com.facebook.presto.kafka.decoder;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

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
    public KafkaRow decodeRow(byte[] data, List<KafkaFieldDecoder<?>> fieldDecoders, List<KafkaColumnHandle> columnHandles)
    {
        return new KafkaCsvRow(data, fieldDecoders, columnHandles);
    }

    public static class KafkaCsvRow
            implements KafkaRow
    {
        private final String[] cache;
        private final List<KafkaFieldDecoder<?>> fieldDecoders;
        private final List<KafkaColumnHandle> columnHandles;

        KafkaCsvRow(byte[] data, List<KafkaFieldDecoder<?>> fieldDecoders, List<KafkaColumnHandle> columnHandles)
        {
            this.cache = new String[columnHandles.size()];
            this.fieldDecoders = checkNotNull(fieldDecoders, "fieldDecoders is null");
            this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

            String line = new String(data, StandardCharsets.UTF_8);

            int corruptedFieldIndex = -1;
            boolean corrupted = false;
            String[] fields = null;

            try {
                fields = PARSER.parseLine(line);
            }
            catch (Exception e) {
                corrupted = true;
            }

            int fieldIndex = 0;
            int cacheIndex = 0;

            for (KafkaColumnHandle columnHandle : columnHandles) {
                // add the full line as _message
                if (columnHandle.getColumn().getName().equals(COLUMN_MESSAGE)) {
                    cache[cacheIndex] = line;
                }
                // add the corrupt / not corrupt flag as _corrupt
                else if (columnHandle.getColumn().getName().equals(COLUMN_CORRUPT)) {
                    cache[cacheIndex] = Boolean.toString(corrupted);
                }
                else {
                    if (fields != null && fieldIndex < fields.length) {
                        cache[cacheIndex] = fields[fieldIndex++];
                    }
                }
                cacheIndex++;
            }
        }

        @Override
        public boolean getBoolean(int field)
        {
            KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeBoolean(cache[field], columnHandle.getColumn().getFormat());
        }

        @Override
        public long getLong(int field)
        {
            KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeLong(cache[field], columnHandle.getColumn().getFormat());
        }

        @Override
        public double getDouble(int field)
        {
            KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeDouble(cache[field], columnHandle.getColumn().getFormat());
        }

        @Override
        public Slice getSlice(int field)
        {
            KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.decodeSlice(cache[field], columnHandle.getColumn().getFormat());
        }

        @Override
        public boolean isNull(int field)
        {
            KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
            KafkaColumnHandle columnHandle = columnHandles.get(field);
            return fieldDecoder.isNull(cache[field], columnHandle.getColumn().getFormat());
        }
    }

    public static class KafkaCsvFieldDecoder
            implements KafkaFieldDecoder<String>
    {
        @Override
        public Set<Class<?>> getJavaTypes()
        {
            return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
        }

        @Override
        public String getRowDecoderName()
        {
            return NAME;
        }

        @Override
        public String getFieldDecoderName()
        {
            return KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
        }

        @Override
        public boolean decodeBoolean(String value, String format)
        {
            try {
                return value == null ? false : Boolean.parseBoolean(value);
            }
            catch (Exception e) {
                return false;
            }
        }

        @Override
        public long decodeLong(String value, String format)
        {
            try {
                return value == null ? 0L : Long.parseLong(value);
            }
            catch (Exception e) {
                return 0L;
            }
        }

        @Override
        public double decodeDouble(String value, String format)
        {
            try {
                return value == null ? 0.0d : Double.parseDouble(value);
            }
            catch (Exception e) {
                return 0.0d;
            }
        }

        @Override
        public Slice decodeSlice(String value, String format)
        {
            return value == null ? Slices.EMPTY_SLICE : Slices.utf8Slice(value);
        }

        @Override
        public boolean isNull(String value, String format)
        {
            return value == null;
        }
    }
}
