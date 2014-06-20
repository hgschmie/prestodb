package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.Type;

import java.util.Map;

public interface KafkaDecoder
{
    String MESSAGE_WILDCARD = "*";

    String MESSAGE_CORRUPTED = "/corrupt";

    KafkaRow decodeRow(byte[] data, Map<String, Type> typeMap);
}
