package com.facebook.presto.kafka.decoder.raw;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaErrorCode;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class RawKafkaFieldDecoder
        implements KafkaFieldDecoder<byte[]>
{
    public static enum FieldType
    {
        BYTE(Byte.SIZE), SHORT(Short.SIZE), INT(Integer.SIZE), LONG(Long.SIZE), FLOAT(Float.SIZE), DOUBLE(Double.SIZE);

        private final int size;

        private FieldType(int bitSize)
        {
            this.size = bitSize / 8;
        }

        public int getSize()
        {
            return size;
        }

        static FieldType forString(String value)
        {
            if (value != null) {
                for (FieldType fieldType : values()) {
                    if (value.toUpperCase(Locale.ENGLISH).equals(fieldType.name())) {
                        return fieldType;
                    }
                }
            }

            return null;
        }
    }

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return RawKafkaRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KafkaFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public KafkaFieldValueProvider decode(final byte[] value, final KafkaColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(value, "value is null");

        String formatHint = columnHandle.getFormatHint();
        FieldType fieldType = columnHandle.getMapping() == null ? FieldType.BYTE : FieldType.forString(columnHandle.getMapping());

        int start = 0;
        int end = value.length;

        if (formatHint != null) {
            List<String> fields = ImmutableList.copyOf(Splitter.on(':').limit(2).split(formatHint));
            if (fields.size() > 0) {
                start = Integer.parseInt(fields.get(0));
                checkState(start >= 0 && start < value.length, "Found start %s, but only 0..%s is legal", start, value.length);
                if (fields.size() > 1) {
                    end = Integer.parseInt(fields.get(1));
                    checkState(end > 0 && end <= value.length, "Found end %s, but only 1..%s is legal", end, value.length);
                }
            }
        }

        checkState(start <= end, "Found start %s and end %s. start must be smaller than end", start, end);

        return new RawKafkaValueProvider(Slices.wrappedBuffer(value, start, end - start), columnHandle, fieldType);
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }

    public static class RawKafkaValueProvider
            extends KafkaFieldValueProvider
    {
        protected final Slice value;
        protected final KafkaColumnHandle columnHandle;
        protected final FieldType fieldType;

        public RawKafkaValueProvider(Slice value, KafkaColumnHandle columnHandle, FieldType fieldType)
        {
            checkState(value.length() >= fieldType.getSize(), "minimum byte size is %s, found %s,", fieldType.getSize(), value.length());
            this.value = value;
            this.columnHandle = columnHandle;
            this.fieldType = fieldType;
        }

        @Override
        public final boolean accept(KafkaColumnHandle columnHandle)
        {
            return this.columnHandle.equals(columnHandle);
        }

        @Override
        public final boolean isNull()
        {
            return value.length() == 0;
        }

        @Override
        public boolean getBoolean()
        {
            if (isNull()) {
                return false;
            }
            switch (fieldType) {
                case BYTE:
                    return value.getByte(0) != 0;
                case SHORT:
                    return value.getShort(0) != 0;
                case INT:
                    return value.getInt(0) != 0;
                case LONG:
                    return value.getLong(0) != 0;
                default:
                    throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), format("conversion %s to boolean not supported", fieldType));
            }
        }

        public long getLong()
        {
            if (isNull()) {
                return 0L;
            }
            switch (fieldType) {
                case BYTE:
                    return value.getByte(0);
                case SHORT:
                    return value.getShort(0);
                case INT:
                    return value.getInt(0);
                case LONG:
                    return value.getLong(0);
                default:
                    throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), format("conversion %s to long not supported", fieldType));
            }
        }

        public double getDouble()
        {
            if (isNull()) {
                return 0.0d;
            }
            switch (fieldType) {
                case FLOAT:
                    return value.getFloat(0);
                case DOUBLE:
                    return value.getDouble(0);
                default:
                    throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), format("conversion %s to double not supported", fieldType));
            }
        }

        public Slice getSlice()
        {
            if (isNull()) {
                return Slices.EMPTY_SLICE;
            }

            if (fieldType == FieldType.BYTE) {
                return value;
            }

            throw new PrestoException(KafkaErrorCode.KAFKA_CONVERSION_NOT_SUPPORTED.toErrorCode(), format("conversion %s to Slice not supported", fieldType));
        }
    }
}
