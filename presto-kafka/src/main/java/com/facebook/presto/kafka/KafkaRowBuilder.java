package com.facebook.presto.kafka;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaRowBuilder
{
    private final Set<KafkaFieldValueProvider> fieldValueProviders;

    KafkaRowBuilder()
    {
        this.fieldValueProviders = new HashSet<>();
    }

    public KafkaRowBuilder add(KafkaFieldValueProvider fieldValueProvider)
    {
        checkNotNull(fieldValueProvider, "fieldValueProvider is null");
        this.fieldValueProviders.add(fieldValueProvider);

        return this;
    }

    public KafkaRowBuilder addAll(Iterable<KafkaFieldValueProvider> fieldValueProviders)
    {
        checkNotNull(fieldValueProviders, "fieldValueProviders is null");
        for (KafkaFieldValueProvider fieldValueProvider : fieldValueProviders) {
            add(fieldValueProvider);
        }

        return this;
    }

    public KafkaRow build(List<KafkaColumnHandle> columnHandles)
    {
        checkNotNull(columnHandles, "columnHandles is null");

        return new KafkaRow(columnHandles, fieldValueProviders);
    }
}
