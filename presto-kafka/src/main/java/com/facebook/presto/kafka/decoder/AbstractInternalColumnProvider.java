package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.slice.Slice;

public abstract class AbstractInternalColumnProvider
        implements InternalColumnProvider
{
    @Override
    public boolean accept(KafkaColumnHandle columnHandle)
    {
        return false;
    }

    @Override
    public boolean getBoolean()
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public long getLong()
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public double getDouble()
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public Slice getSlice()
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public boolean isNull()
    {
        return true;
    }
}