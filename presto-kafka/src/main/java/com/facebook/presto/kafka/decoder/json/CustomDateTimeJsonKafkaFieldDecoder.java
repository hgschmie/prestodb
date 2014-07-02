package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaErrorCode;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Custom date format decoder.
 * <p/>
 * When the column type is BIGINT or timestamp: Converts JSON string or long fields into timestamp compatible long values using the format given in the <tt>formatHint</tt> field of the field definition.
 * When the column type is VARCHAR: Converts JSON string or long fields into a formatted string using the format given in the <tt>formatHint</tt> field of the field definition.
 * <p/>
 * <tt>formatHint</tt> uses {@link org.joda.time.format.DateTimeFormatter} format.
 * <p/>
 * Uses hardcoded UTC timezone and english locale.
 */
public class CustomDateTimeJsonKafkaFieldDecoder
        extends JsonKafkaFieldDecoder
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return "custom-date-time";
    }

    @Override
    public KafkaFieldValueProvider decode(final JsonNode value, final KafkaColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(value, "value is null");

        return new CustomDateTImeJsonKafkaValueProvider(value, columnHandle);
    }

    public static class CustomDateTImeJsonKafkaValueProvider
            extends JsonKafkaValueProvider
    {
        public CustomDateTImeJsonKafkaValueProvider(JsonNode value, KafkaColumnHandle columnHandle)
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

            checkNotNull(columnHandle.getFormatHint(), "formatHint is null");
            String textValue = value.isValueNode() ? value.asText() : value.toString();

            DateTimeFormatter formatter = DateTimeFormat.forPattern(columnHandle.getFormatHint()).withLocale(Locale.ENGLISH).withZoneUTC();
            return formatter.parseMillis(value.asText());
        }

        @Override
        public Slice getSlice()
        {
            if (isNull()) {
                return Slices.EMPTY_SLICE;
            }

            checkNotNull(columnHandle.getFormatHint(), "formatHint is null");

            DateTimeFormatter formatter = DateTimeFormat.forPattern(columnHandle.getFormatHint()).withLocale(Locale.ENGLISH).withZoneUTC();
            return Slices.utf8Slice(formatter.print(value.asLong()));
        }
    }
}
