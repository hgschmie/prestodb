package com.facebook.presto.kafka.decoder.json;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Set;

public class ISO8601JsonKafkaFieldDecoder
        extends JsonKafkaFieldDecoder
{
    private static final DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser().withZoneUTC();

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return "iso8601";
    }

    @Override
    public boolean decodeBoolean(JsonNode value, String format)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public double decodeDouble(JsonNode value, String format)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR.toErrorCode(), "conversion not supported");
    }

    @Override
    public long decodeLong(JsonNode value, String format)
    {
        if (value == null) {
            return 0;
        }

        return formatter.parseMillis(value.asText());
    }
}
