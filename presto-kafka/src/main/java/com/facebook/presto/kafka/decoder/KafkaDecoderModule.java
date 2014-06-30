package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.decoder.csv.CsvKafkaDecoderModule;
import com.facebook.presto.kafka.decoder.dummy.DummyKafkaDecoderModule;
import com.facebook.presto.kafka.decoder.json.JsonKafkaDecoderModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

public class KafkaDecoderModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(KafkaDecoderRegistry.class).in(Scopes.SINGLETON);

        binder.install(new DummyKafkaDecoderModule());
        binder.install(new CsvKafkaDecoderModule());
        binder.install(new JsonKafkaDecoderModule());
    }

    public static void bindRowDecoder(Binder binder, Class<? extends KafkaRowDecoder> decoderClass)
    {
        Multibinder<KafkaRowDecoder> rowDecoderBinder = Multibinder.newSetBinder(binder, KafkaRowDecoder.class);
        rowDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }

    public static void bindFieldDecoder(Binder binder, Class<? extends KafkaFieldDecoder> decoderClass)
    {
        Multibinder<KafkaFieldDecoder> fieldDecoderBinder = Multibinder.newSetBinder(binder, KafkaFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(decoderClass).in(Scopes.SINGLETON);
    }
}
