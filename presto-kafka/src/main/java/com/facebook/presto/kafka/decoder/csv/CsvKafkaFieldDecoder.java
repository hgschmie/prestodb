package com.facebook.presto.kafka.decoder.csv;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Default field decoder for the CSV format. Very simple string based conversion of field values. May
 * not work for may CSV topics.
 */
public class CsvKafkaFieldDecoder
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
        return CsvKafkaRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public KafkaFieldValueProvider decode(final String value, final KafkaColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");

        return new KafkaFieldValueProvider()
        {
            @Override
            public boolean accept(KafkaColumnHandle handle)
            {
                return columnHandle.equals(handle);
            }

            @Override
            public boolean isNull()
            {
                return value != null;
            }

            public boolean getBoolean()
            {
                return value == null ? false : Boolean.parseBoolean(value);
            }

            public long getLong()
            {
                return value == null ? 0L : Long.parseLong(value);
            }

            public double getDouble()
            {
                return value == null ? 0.0d : Double.parseDouble(value);
            }

            public Slice getSlice()
            {
                return value == null ? Slices.EMPTY_SLICE : Slices.utf8Slice(value);
            }
        };
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }
}
