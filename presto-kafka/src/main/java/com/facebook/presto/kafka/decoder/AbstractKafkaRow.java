package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractKafkaRow
        implements KafkaRow
{
    private final List<KafkaColumnHandle> columnHandles;
    private final InternalColumnProvider[] internalColumnCache;

    protected AbstractKafkaRow(List<KafkaColumnHandle> columnHandles, Set<InternalColumnProvider> internalColumnProviders)
    {
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        this.internalColumnCache = new InternalColumnProvider[columnHandles.size()];

        for (int i = 0; i < columnHandles.size(); i++) {
            for (InternalColumnProvider provider : internalColumnProviders) {
                if (provider.accept(columnHandles.get(i))) {
                    internalColumnCache[i] = provider;
                    break; // for(InternalColumnProvider...
                }
            }
        }
    }

    @Override
    public final boolean getBoolean(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            checkState(internalColumnCache[field] != null, "No value provider for column %s found!", columnHandle.getName());
            return internalColumnCache[field].getBoolean();
        }
        else {
            return getBoolean(columnHandle, field);
        }
    }

    @Override
    public final long getLong(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            checkState(internalColumnCache[field] != null, "No value provider for column %s found!", columnHandle.getName());
            return internalColumnCache[field].getLong();
        }
        else {
            return getLong(columnHandle, field);
        }
    }

    @Override
    public final double getDouble(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            checkState(internalColumnCache[field] != null, "No value provider for column %s found!", columnHandle.getName());
            return internalColumnCache[field].getDouble();
        }
        else {
            return getDouble(columnHandle, field);
        }
    }

    @Override
    public final Slice getSlice(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            checkState(internalColumnCache[field] != null, "No value provider for column %s found!", columnHandle.getName());
            return internalColumnCache[field].getSlice();
        }
        else {
            return getSlice(columnHandle, field);
        }
    }

    @Override
    public final boolean isNull(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            return internalColumnCache[field] == null || internalColumnCache[field].isNull();
        }
        else {
            return isNull(columnHandle, field);
        }
    }

    protected abstract boolean getBoolean(KafkaColumnHandle columnHandle, int field);

    protected abstract long getLong(KafkaColumnHandle columnHandle, int field);

    protected abstract double getDouble(KafkaColumnHandle columnHandle, int field);

    protected abstract Slice getSlice(KafkaColumnHandle columnHandle, int field);

    protected abstract boolean isNull(KafkaColumnHandle columnHandle, int field);
}
