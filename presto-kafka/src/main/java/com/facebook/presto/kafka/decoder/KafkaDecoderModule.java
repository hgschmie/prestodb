package com.facebook.presto.kafka.decoder;

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

        Multibinder<KafkaRowDecoder> rowDecoderBinder = Multibinder.newSetBinder(binder, KafkaRowDecoder.class);
        rowDecoderBinder.addBinding().to(JsonKafkaRowDecoder.class).in(Scopes.SINGLETON);
        rowDecoderBinder.addBinding().to(CsvKafkaRowDecoder.class).in(Scopes.SINGLETON);

        Multibinder<KafkaFieldDecoder> fieldDecoderBinder = Multibinder.newSetBinder(binder, KafkaFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(CsvKafkaRowDecoder.KafkaCsvFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(JsonKafkaRowDecoder.KafkaJsonFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(TimestampJsonKafkaFieldDecoders.ISO8601JsonKafkaFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(TimestampJsonKafkaFieldDecoders.RFC2822JsonKafkaFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(TimestampJsonKafkaFieldDecoders.SecondsSinceEpochJsonKafkaFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(TimestampJsonKafkaFieldDecoders.MilliSecondsSinceEpochJsonKafkaFieldDecoder.class);
        fieldDecoderBinder.addBinding().to(TimestampJsonKafkaFieldDecoders.CustomDateTimeJsonKafkaFieldDecoder.class);
    }
}
