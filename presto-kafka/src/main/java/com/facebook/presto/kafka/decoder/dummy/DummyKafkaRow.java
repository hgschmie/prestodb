package com.facebook.presto.kafka.decoder.dummy;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalColumnProvider;
import com.facebook.presto.kafka.decoder.AbstractKafkaRow;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Set;

public class DummyKafkaRow
        extends AbstractKafkaRow

{
    public DummyKafkaRow(List<KafkaColumnHandle> columnHandles, Set<KafkaInternalColumnProvider> internalColumnProviders)
    {
        super(columnHandles, internalColumnProviders);
    }

    @Override
    protected boolean getBoolean(KafkaColumnHandle columnHandle, int field)
    {
        return false;
    }

    @Override
    protected long getLong(KafkaColumnHandle columnHandle, int field)
    {
        return 0;
    }

    @Override
    protected double getDouble(KafkaColumnHandle columnHandle, int field)
    {
        return 0;
    }

    @Override
    protected Slice getSlice(KafkaColumnHandle columnHandle, int field)
    {
        return null;
    }

    @Override
    protected boolean isNull(KafkaColumnHandle columnHandle, int field)
    {
        return false;
    }
}
