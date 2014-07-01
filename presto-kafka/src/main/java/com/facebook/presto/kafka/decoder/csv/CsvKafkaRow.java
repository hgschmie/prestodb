package com.facebook.presto.kafka.decoder.csv;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaInternalFieldValueProvider;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * CSV format specific representation of a Kafka message.
 */
public class CsvKafkaRow
        extends KafkaRow
{
    private final String[] cache;
    private final List<KafkaColumnHandle> columnHandles;
    private final Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders;

    CsvKafkaRow(String[] fields, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders, Set<KafkaInternalFieldValueProvider> internalFieldValueProviders)
    {
        super(columnHandles, internalFieldValueProviders);

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
        @SuppressWarnings("unchecked")
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeBoolean(cache[field], columnHandle.getFormatHint());
    }

    @Override
    public long getLong(KafkaColumnHandle columnHandle, int field)
    {
        @SuppressWarnings("unchecked")
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeLong(cache[field], columnHandle.getFormatHint());
    }

    @Override
    public double getDouble(KafkaColumnHandle columnHandle, int field)
    {
        @SuppressWarnings("unchecked")
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeDouble(cache[field], columnHandle.getFormatHint());
    }

    @Override
    public Slice getSlice(KafkaColumnHandle columnHandle, int field)
    {
        @SuppressWarnings("unchecked")
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(columnHandle);
        return fieldDecoder.decodeSlice(cache[field], columnHandle.getFormatHint());
    }

    @Override
    public boolean isNull(KafkaColumnHandle columnHandle, int field)
    {
        @SuppressWarnings("unchecked")
        KafkaFieldDecoder<String> fieldDecoder = (KafkaFieldDecoder<String>) fieldDecoders.get(columnHandle);
        return fieldDecoder.isNull(cache[field], columnHandle.getFormatHint());
    }
}
