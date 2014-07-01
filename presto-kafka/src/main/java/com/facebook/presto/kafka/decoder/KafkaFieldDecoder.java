package com.facebook.presto.kafka.decoder;

import io.airlift.slice.Slice;

import java.util.Set;

/**
 * Format specific field decoder description.
 */
public interface KafkaFieldDecoder<T>
{
    /** Default name. Each decoder type *must* have a default decoder as fallback. */
    String DEFAULT_FIELD_DECODER_NAME = "_default";

    /** Returns the types which the field decoder can process. */
    public Set<Class<?>> getJavaTypes();

    /** Returns the name of the row decoder to which this field decoder belongs. */
    public String getRowDecoderName();

    /** Returns the field decoder specific name. This name will be selected with the {@link com.facebook.presto.kafka.KafkaTopicFieldDescription#dataFormat} value. */
    public String getFieldDecoderName();

    /**
     * Decode an internal value into a boolean. Can throw a {@link com.facebook.presto.kafka.KafkaErrorCode#KAFKA_CONVERSION_NOT_SUPPORTED} exception
     * if the {@link KafkaFieldDecoder#getJavaTypes()} result does not contain {@link com.facebook.presto.spi.type.BooleanType#BOOLEAN}.
     */
    boolean decodeBoolean(T value, String formatHint);

    /**
     * Decode an internal value into a long. Can throw a {@link com.facebook.presto.kafka.KafkaErrorCode#KAFKA_CONVERSION_NOT_SUPPORTED} exception
     * if the {@link KafkaFieldDecoder#getJavaTypes()} result does not contain {@link com.facebook.presto.spi.type.BigintType#BIGINT}.
     */
    long decodeLong(T value, String formatHint);

    /**
     * Decode an internal value into a double. Can throw a {@link com.facebook.presto.kafka.KafkaErrorCode#KAFKA_CONVERSION_NOT_SUPPORTED} exception
     * if the {@link KafkaFieldDecoder#getJavaTypes()} result does not contain {@link com.facebook.presto.spi.type.DoubleType#DOUBLE}.
     */
    double decodeDouble(T value, String formatHint);

    /**
     * Decode an internal value into a Slice. Can throw a {@link com.facebook.presto.kafka.KafkaErrorCode#KAFKA_CONVERSION_NOT_SUPPORTED} exception
     * if the {@link KafkaFieldDecoder#getJavaTypes()} result does not contain {@link com.facebook.presto.spi.type.VarcharType#VARCHAR}.
     */
    Slice decodeSlice(T value, String formatHint);

    /** Do null check on an internal value. */
    boolean isNull(T value, String formatHint);
}
