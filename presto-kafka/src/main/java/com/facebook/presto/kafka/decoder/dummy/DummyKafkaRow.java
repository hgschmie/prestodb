package com.facebook.presto.kafka.decoder.dummy;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldValueProvider;
import com.facebook.presto.kafka.KafkaRow;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Set;

/**
 * Represents a Message decoded in the 'dummy' format.
 */
public class DummyKafkaRow
        extends KafkaRow
{
    public DummyKafkaRow(List<KafkaColumnHandle> columnHandles, Set<KafkaInternalFieldValueProvider> internalFieldValueProviders)
    {
        super(columnHandles, internalFieldValueProviders);
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
