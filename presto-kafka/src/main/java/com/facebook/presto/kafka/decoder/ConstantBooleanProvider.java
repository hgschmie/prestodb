package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConstantBooleanProvider
        extends AbstractInternalColumnProvider
{
    private final boolean value;
    private final String fieldName;

    public ConstantBooleanProvider(String fieldName, boolean value)
    {
        this.fieldName = checkNotNull(fieldName, "fieldName is null");
        this.value = value;
    }

    @Override
    public boolean accept(KafkaColumnHandle columnHandle)
    {
        return columnHandle.getName().equals(fieldName);
    }

    @Override
    public boolean getBoolean()
    {
        return value;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }
}
