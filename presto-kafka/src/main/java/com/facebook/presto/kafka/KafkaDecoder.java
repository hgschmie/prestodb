package com.facebook.presto.kafka;

public interface KafkaDecoder
{
    KafkaRow decodeRow(byte [] data);
}
