package com.facebook.presto.kafka.decoder.dummy;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindFieldDecoder;
import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindRowDecoder;

/**
 * Guice module for the 'dummy' decoder. See {@link com.facebook.presto.kafka.decoder.dummy.DummyKafkaRowDecoder} for an explanation.
 */
public class DummyKafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, DummyKafkaRowDecoder.class);

        bindFieldDecoder(binder, DummyKafkaFieldDecoder.class);
    }
}
