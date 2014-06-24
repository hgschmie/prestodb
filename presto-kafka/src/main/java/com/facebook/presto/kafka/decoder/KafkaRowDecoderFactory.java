package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import kafka.Kafka;

import java.util.List;

public interface KafkaRowDecoderFactory
{
    String getName();

    KafkaRowDecoder buildRowDecoder(List<KafkaColumnHandle> columnHandles);
}
