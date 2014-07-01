package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaErrorCode;
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
 *
 * When the column type is BIGINT or timestamp: Converts JSON string or long fields into timestamp compatible long values using the format given in the <tt>formatHint</tt> field of the field definition.
 * When the column type is VARCHAR: Converts JSON string or long fields into a formatted string using the format given in the <tt>formatHint</tt> field of the field definition.
 *
 * <tt>formatHint</tt> uses {@link org.joda.time.format.DateTimeFormatter} format.
 *
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
    public boolean decodeBoolean(JsonNode value, String formatHint)
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to boolean not supported");
    }

    @Override
    public double decodeDouble(JsonNode value, String formatHint)
    {
        throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), "conversion to double not supported");
    }

    @Override
    public long decodeLong(JsonNode value, String formatHint)
    {
        checkNotNull(value, "value is null");
        checkNotNull(formatHint, "formatHint is null");

        if (isNull(value, formatHint)) {
            return 0L;
        }

        DateTimeFormatter formatter = DateTimeFormat.forPattern(formatHint).withLocale(Locale.ENGLISH).withZoneUTC();
        return formatter.parseMillis(value.asText());
    }

    @Override
    public Slice decodeSlice(JsonNode value, String formatHint)
    {
        checkNotNull(value, "value is null");
        checkNotNull(formatHint, "formatHint is null");

        if (isNull(value, formatHint)) {
            return Slices.EMPTY_SLICE;
        }

        DateTimeFormatter formatter = DateTimeFormat.forPattern(formatHint).withLocale(Locale.ENGLISH).withZoneUTC();
        return Slices.utf8Slice(formatter.print(value.asLong()));
    }
}
