package com.facebook.presto.kafka.decoder;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;

import java.util.Set;

/**
 * Format specific field decoder description.
 */
public interface KafkaFieldDecoder<T>
{
    /**
     * Default name. Each decoder type *must* have a default decoder as fallback.
     */
    String DEFAULT_FIELD_DECODER_NAME = "_default";

    /**
     * Returns the types which the field decoder can process.
     */
    public Set<Class<?>> getJavaTypes();

    /**
     * Returns the name of the row decoder to which this field decoder belongs.
     */
    public String getRowDecoderName();

    /**
     * Returns the field decoder specific name. This name will be selected with the {@link com.facebook.presto.kafka.KafkaTopicFieldDescription#dataFormat} value.
     */
    public String getFieldDecoderName();

    KafkaFieldValueProvider decode(T value, KafkaColumnHandle columnHandle);
}
