package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.kafka.KafkaErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Seconds since the epoch date format decoder.
 * <p/>
 * Converts JSON long and string fields into timestamp compatible long values when the presto column definition is a TIMESTAMP or BIGINT.
 * Converts JSON long and string fields into ISO8601 formatted time strings when the presto column definition is a VARCHAR.
 * <p/>
 * Uses hardcoded UTC timezone and english locale.
 */
public class SecondsSinceEpochJsonKafkaFieldDecoder
        extends JsonKafkaFieldDecoder
{
    /**
     * Todo - configurable time zones and locales.
     */
    private static final DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser().withLocale(Locale.ENGLISH).withZoneUTC();

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return "seconds-since-epoch";
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

        return isNull(value, formatHint) ? 0L : value.asLong() * 1000L;
    }

    @Override
    public Slice decodeSlice(JsonNode value, String formatHint)
    {
        checkNotNull(value, "value is null");

        return isNull(value, formatHint) ? Slices.EMPTY_SLICE : Slices.utf8Slice(formatter.print(value.asLong() * 1000L));
    }
}
