package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaRow;
import com.facebook.presto.kafka.KafkaSplit;

import java.util.List;

public interface KafkaRowDecoder
{
    String COLUMN_MESSAGE = "_message";

    String COLUMN_CORRUPT = "_corrupt";

    String getName();

    KafkaRow decodeRow(byte[] data,
            List<KafkaFieldDecoder<?>> fieldDecoders,
            List<KafkaColumnHandle> columnHandles,
            KafkaSplit split,
            long messageOffset,
            long messageCount);
}
