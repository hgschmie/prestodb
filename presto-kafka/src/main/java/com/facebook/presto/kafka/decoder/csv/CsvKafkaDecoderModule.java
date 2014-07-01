package com.facebook.presto.kafka.decoder.csv;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindFieldDecoder;
import static com.facebook.presto.kafka.decoder.KafkaDecoderModule.bindRowDecoder;

/**
 * Guice module for the CSV decoder.
 */
public class CsvKafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, CsvKafkaRowDecoder.class);

        bindFieldDecoder(binder, CsvKafkaFieldDecoder.class);
    }
}
