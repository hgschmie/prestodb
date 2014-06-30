package com.facebook.presto.kafka.decoder.csv;

import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Set;

import static java.lang.String.format;

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

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }
}
