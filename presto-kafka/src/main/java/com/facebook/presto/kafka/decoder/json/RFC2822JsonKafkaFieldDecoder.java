package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaErrorCode;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * RFC 2822 date format decoder.
 * <p/>
 * Converts JSON string fields formatted in RFC 2822 format into timestamp compatible long values when the presto column definition is a TIMESTAMP or BIGINT.
 * Returns the value of the JSON string field as is when the presto column definition is a VARCHAR.
 * <p/>
 * Uses hardcoded UTC timezone and english locale.
 */
public class RFC2822JsonKafkaFieldDecoder
        extends JsonKafkaFieldDecoder
{
    /**
     * Todo - configurable time zones and locales.
     */
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH).withZoneUTC();

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return "rfc2822";
    }

    @Override
    public KafkaFieldValueProvider decode(final JsonNode value, final KafkaColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(value, "value is null");

        return new RFC2822JsonKafkaValueProvider(value, columnHandle);
    }

    public static class RFC2822JsonKafkaValueProvider
            extends JsonKafkaValueProvider
    {
        public RFC2822JsonKafkaValueProvider(JsonNode value, KafkaColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        public boolean getBoolean()
        {
            throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to boolean not supported");
        }

        @Override
        public double getDouble()
        {
            throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to double not supported");
        }

        @Override
        public long getLong()
        {
            if (isNull()) {
                return 0L;
            }

            String textValue = value.isValueNode() ? value.asText() : value.toString();
            return formatter.parseMillis(textValue);
        }
    }
}
