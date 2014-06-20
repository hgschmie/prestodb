package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.Type;

import java.util.Map;

public interface KafkaRowDecoder
{
    String MESSAGE_WILDCARD = "*";

    String MESSAGE_CORRUPTED = "corrupt";

    String getName();

    KafkaRow decodeRow(byte[] data, Map<String, Type> typeMap);
}
