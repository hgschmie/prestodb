package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Set;

public class RFC2822JsonKafkaFieldDecoder
        extends JsonKafkaFieldDecoder
{
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
    public boolean decodeBoolean(JsonNode value, String formatHint)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public double decodeDouble(JsonNode value, String formatHint)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public long decodeLong(JsonNode value, String formatHint)
    {
        if (value == null) {
            return 0;
        }

        return formatter.parseMillis(value.asText());
    }
}
