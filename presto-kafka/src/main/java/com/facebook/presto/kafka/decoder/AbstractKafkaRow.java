package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalColumnProvider;
import com.facebook.presto.kafka.KafkaRow;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractKafkaRow
        implements KafkaRow
{
    private final List<KafkaColumnHandle> columnHandles;
    private final KafkaInternalColumnProvider[] internalColumnCache;

    protected AbstractKafkaRow(List<KafkaColumnHandle> columnHandles, Set<KafkaInternalColumnProvider> internalColumnProviders)
    {
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        this.internalColumnCache = new KafkaInternalColumnProvider[columnHandles.size()];

        // If a value provider for a requested internal column is present, assign the
        // value to the internal cache. It is possible that an internal column is present
        // where no value provider exists (e.g. the '_corrupt' column with the DummyRowDecoder).
        // In that case, the cache is null (and the column is reported as null).
        for (int i = 0; i < columnHandles.size(); i++) {
            if (columnHandles.get(i).isInternal()) {
                for (KafkaInternalColumnProvider provider : internalColumnProviders) {
                    if (provider.accept(columnHandles.get(i))) {
                        internalColumnCache[i] = provider;
                        break; // for(InternalColumnProvider...
                    }
                }
            }
        }
    }

    @Override
    public final boolean getBoolean(int field)
    {
        KafkaColumnHandle columnHandle = columnHandles.get(field);
        if (columnHandle.isInternal()) {
            return internalColumnCache[field] == null ? false : internalColumnCache[field].getBoolean();
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
            return internalColumnCache[field] == null ? 0L : internalColumnCache[field].getLong();
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
            return internalColumnCache[field] == null ? 0.0d : internalColumnCache[field].getDouble();
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
            return internalColumnCache[field] == null ? Slices.EMPTY_SLICE : internalColumnCache[field].getSlice();
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
