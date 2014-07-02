package com.facebook.presto.kafka.decoder.raw;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindFieldDecoder;
import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindRowDecoder;

public class RawKafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, RawKafkaRowDecoder.class);

        bindFieldDecoder(binder, RawKafkaFieldDecoder.class);
    }
}
