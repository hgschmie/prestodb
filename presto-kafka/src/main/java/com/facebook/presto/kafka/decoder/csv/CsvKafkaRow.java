package com.facebook.presto.kafka.decoder.csv;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalColumnProvider;
import com.facebook.presto.kafka.decoder.AbstractKafkaRow;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class CsvKafkaRow
        extends AbstractKafkaRow
{
    private final String[] cache;
    private final List<KafkaColumnHandle> columnHandles;
    private final Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders;

    CsvKafkaRow(String[] fields, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders, Set<KafkaInternalColumnProvider> internalColumnProviders)
    {
        super(columnHandles, internalColumnProviders);

        checkNotNull(fields, "fields is null");

        this.fieldDecoders = checkNotNull(fieldDecoders, "fieldDecoders is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
        this.cache = new String[columnHandles.size()];

        int fieldIndex = 0;
        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (fieldIndex >= fields.length) {
                break;
            }
            if (!columnHandle.isInternal()) {
                cache[columnHandle.getOrdinalPosition()] = fields[fieldIndex++];
            }
        }
    }

    @Override
    public boolean getBoolean(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
        return fieldDecoder.decodeBoolean(cache[field], columnHandle.getFormat());
    }

    @Override
    public long getLong(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
        return fieldDecoder.decodeLong(cache[field], columnHandle.getFormat());
    }

    @Override
    public double getDouble(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
        return fieldDecoder.decodeDouble(cache[field], columnHandle.getFormat());
    }

    @Override
    public Slice getSlice(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
        return fieldDecoder.decodeSlice(cache[field], columnHandle.getFormat());
    }

    @Override
    public boolean isNull(KafkaColumnHandle columnHandle, int field)
    {
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(field);
        return fieldDecoder.isNull(cache[field], columnHandle.getFormat());
    }
}
