package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConstantLongProvider
        extends AbstractInternalColumnProvider
{
    private final long value;
    private final String fieldName;

    public ConstantLongProvider(String fieldName, long value)
    {
        this.fieldName = checkNotNull(fieldName);
        this.value = value;
    }

    @Override
    public boolean accept(KafkaColumnHandle columnHandle)
    {
        return columnHandle.getName().equals(fieldName);
    }

    @Override
    public long getLong()
    {
        return value;
    }
}
