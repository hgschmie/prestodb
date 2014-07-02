package com.facebook.presto.kafka;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;

/**
 * Provides values for internal columns. Instances of this class are returned by {@link com.facebook.presto.kafka.KafkaInternalFieldDescription#forBooleanValue(boolean)},
 * {@link com.facebook.presto.kafka.KafkaInternalFieldDescription#forLongValue(long)} and {@link com.facebook.presto.kafka.KafkaInternalFieldDescription#forByteValue(byte[])}.
 */
public abstract class KafkaFieldValueProvider
{
    public abstract boolean accept(KafkaColumnHandle columnHandle);

    public boolean getBoolean()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to boolean not supported");
    }

    public long getLong()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to long not supported");
    }

    public double getDouble()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to double not supported");
    }

    public Slice getSlice()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to Slice not supported");
    }

    public abstract boolean isNull();
}
