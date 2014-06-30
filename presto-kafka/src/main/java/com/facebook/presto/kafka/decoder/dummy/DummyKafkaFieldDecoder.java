package com.facebook.presto.kafka.decoder.dummy;

import com.facebook.presto.kafka.KafkaErrorCode;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Set;

import static java.lang.String.format;

public class DummyKafkaFieldDecoder
        implements KafkaFieldDecoder<Void>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return DummyKafkaRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public boolean decodeBoolean(Void value, String format)
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "dummy decoder can not decode boolean");
    }

    @Override
    public long decodeLong(Void value, String format)
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "dummy decoder can not decode long");
    }

    @Override
    public double decodeDouble(Void value, String format)
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "dummy decoder can not decode double");
    }

    @Override
    public Slice decodeSlice(Void value, String format)
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "dummy decoder can not decode Slice");
    }

    @Override
    public boolean isNull(Void value, String format)
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "dummy decoder can not check for null");
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }
}
