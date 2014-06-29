package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConstantBytesProvider
        extends AbstractInternalColumnProvider
{
    private final String fieldName;
    private final byte[] message;

    public ConstantBytesProvider(String fieldName, byte[] message)
    {
        this.fieldName = checkNotNull(fieldName, "fieldName is null");
        this.message = message;
    }

    @Override
    public boolean accept(KafkaColumnHandle columnHandle)
    {
        return columnHandle.getName().equals(fieldName);
    }

    @Override
    public Slice getSlice()
    {
        return isNull() ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(message);
    }

    @Override
    public boolean isNull()
    {
        return message == null || message.length == 0;
    }
}
