package com.facebook.presto.kafka;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;

/**
 * Subclasses provide values for internal columns.
 */
public abstract class KafkaInternalFieldValueProvider
{
    public abstract boolean accept(KafkaColumnHandle columnHandle);

    public boolean getBoolean()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_OPERATION_NOT_SUPPORTED.toErrorCode(), "conversion to boolean not supported");
    }

    public long getLong()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_OPERATION_NOT_SUPPORTED.toErrorCode(), "conversion to long not supported");
    }

    public double getDouble()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_OPERATION_NOT_SUPPORTED.toErrorCode(), "conversion to double not supported");
    }

    public Slice getSlice()
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_OPERATION_NOT_SUPPORTED.toErrorCode(), "conversion to Slice not supported");
    }

    public abstract boolean isNull();
}
