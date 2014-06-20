package com.facebook.presto.kafka;

import java.util.Map;

import com.facebook.presto.spi.type.Type;

public interface KafkaDecoder
{
    String MESSAGE_WILDCARD = "*";

    String MESSAGE_CORRUPTED = "/corrupt";

    KafkaRow decodeRow(byte[] data, Map<String, Type> typeMap);
}
