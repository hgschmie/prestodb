package com.facebook.presto.kafka;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class KafkaRow
{
    private final List<KafkaColumnHandle> columnHandles;
    private final KafkaInternalFieldValueProvider[] internalFieldValueProviderCache;

    protected KafkaRow(List<KafkaColumnHandle> columnHandles, Set<KafkaInternalFieldValueProvider> internalFieldValueProviders)
    {
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        this.internalFieldValueProviderCache = new KafkaInternalFieldValueProvider[columnHandles.size()];

        // If a value provider for a requested internal column is present, assign the
        // value to the internal cache. It is possible that an internal column is present
        // where no value provider exists (e.g. the '_corrupt' column with the DummyRowDecoder).
        // In that case, the cache is null (and the column is reported as null).
        for (int i = 0; i < columnHandles.size(); i++) {
            if (columnHandles.get(i).isInternal()) {
                for (KafkaInternalFieldValueProvider internalFieldValueProvider : internalFieldValueProviders) {
                    if (internalFieldValueProvider.accept(columnHandles.get(i))) {
                        internalFieldValueProviderCache[i] = internalFieldValueProvider;
                        break; // for(InternalColumnProvider...
                    }
                }
            }
        }
    }

    public final boolean getBoolean(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            return internalFieldValueProviderCache[field] == null ? false : internalFieldValueProviderCache[field].getBoolean();
        }
        else {
            return getBoolean(columnHandle, field);
        }
    }

    public final long getLong(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            return internalFieldValueProviderCache[field] == null ? 0L : internalFieldValueProviderCache[field].getLong();
        }
        else {
            return getLong(columnHandle, field);
        }
    }

    public final double getDouble(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            return internalFieldValueProviderCache[field] == null ? 0.0d : internalFieldValueProviderCache[field].getDouble();
        }
        else {
            return getDouble(columnHandle, field);
        }
    }

    public final Slice getSlice(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            return internalFieldValueProviderCache[field] == null ? Slices.EMPTY_SLICE : internalFieldValueProviderCache[field].getSlice();
        }
        else {
            return getSlice(columnHandle, field);
        }
    }

    public final boolean isNull(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            return internalFieldValueProviderCache[field] == null || internalFieldValueProviderCache[field].isNull();
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
