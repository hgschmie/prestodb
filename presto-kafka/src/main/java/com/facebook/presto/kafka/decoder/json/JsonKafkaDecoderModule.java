package com.facebook.presto.kafka.decoder.json;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindFieldDecoder;
import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindRowDecoder;

public class JsonKafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, JsonKafkaRowDecoder.class);

        bindFieldDecoder(binder, JsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, ISO8601JsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, RFC2822JsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, SecondsSinceEpochJsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, MilliSecondsSinceEpochJsonKafkaFieldDecoder.class);
        bindFieldDecoder(binder, CustomDateTimeJsonKafkaFieldDecoder.class);
    }
}
