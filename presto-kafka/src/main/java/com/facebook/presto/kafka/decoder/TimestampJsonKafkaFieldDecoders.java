package com.facebook.presto.kafka.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TimestampJsonKafkaFieldDecoders
{
    private TimestampJsonKafkaFieldDecoders()
    {
        throw new AssertionError("Do not instantiate");
    }

    public static class ISO8601JsonKafkaFieldDecoder
            extends JsonKafkaRowDecoder.KafkaJsonFieldDecoder
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
            throw new IllegalStateException("can not decode to a boolean");
        }

        @Override
        public double decodeDouble(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a double");
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

    public static class RFC2822JsonKafkaFieldDecoder
            extends JsonKafkaRowDecoder.KafkaJsonFieldDecoder
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
        public boolean decodeBoolean(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a boolean");
        }

        @Override
        public double decodeDouble(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a double");
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

    public static class SecondsSinceEpochJsonKafkaFieldDecoder
            extends JsonKafkaRowDecoder.KafkaJsonFieldDecoder
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
            return "seconds-since-epoch";
        }

        @Override
        public boolean decodeBoolean(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a boolean");
        }

        @Override
        public double decodeDouble(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a double");
        }

        @Override
        public long decodeLong(JsonNode value, String format)
        {
            if (value == null) {
                return 0;
            }

            return value.asLong() * 1000L;
        }
    }

    public static class MilliSecondsSinceEpochJsonKafkaFieldDecoder
            extends JsonKafkaRowDecoder.KafkaJsonFieldDecoder
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
            return "milliseconds-since-epoch";
        }

        @Override
        public boolean decodeBoolean(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a boolean");
        }

        @Override
        public double decodeDouble(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a double");
        }

        @Override
        public long decodeLong(JsonNode value, String format)
        {
            if (value == null) {
                return 0;
            }

            return value.asLong();
        }
    }

    public static class CustomDateTimeJsonKafkaFieldDecoder
            extends JsonKafkaRowDecoder.KafkaJsonFieldDecoder
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
            return "milliseconds-since-epoch";
        }

        @Override
        public boolean decodeBoolean(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a boolean");
        }

        @Override
        public double decodeDouble(JsonNode value, String format)
        {
            throw new IllegalStateException("can not decode to a double");
        }

        @Override
        public long decodeLong(JsonNode value, String format)
        {
            checkNotNull(format, "format is null");

            if (value == null) {
                return 0;
            }

            DateTimeFormatter formatter = DateTimeFormat.forPattern(format).withLocale(Locale.ENGLISH).withZoneUTC();

            return formatter.parseMillis(value.asText());
        }
    }
}
