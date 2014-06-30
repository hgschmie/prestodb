package com.facebook.presto.kafka;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Describes an internal (managed by the connector) field which is added to each table row. The definition itself makes the row
 * show up in the tables (the columns are hidden, so they must be explicitly selected) but unless the field is hooked in using the
 * forBooleanValue/forLongValue/forBytesValue methods and the resulting FieldValueProvider is then passed into the appropriate row decoder, the fields
 * will be null. Most values are assigned in the {@link com.facebook.presto.kafka.KafkaRecordSet}.
 */
public class KafkaInternalFieldDescription
{
    public static final KafkaInternalFieldDescription MESSAGE_FIELD = new KafkaInternalFieldDescription("_message", VarcharType.VARCHAR);
    public static final KafkaInternalFieldDescription CORRUPTED_FIELD = new KafkaInternalFieldDescription("_corrupted", BooleanType.BOOLEAN);
    public static final KafkaInternalFieldDescription PARTITION_ID_FIELD = new KafkaInternalFieldDescription("_partition_id", BigintType.BIGINT);
    public static final KafkaInternalFieldDescription SEGMENT_START_FIELD = new KafkaInternalFieldDescription("_segment_start", BigintType.BIGINT);
    public static final KafkaInternalFieldDescription SEGMENT_END_FIELD = new KafkaInternalFieldDescription("_segment_end", BigintType.BIGINT);
    public static final KafkaInternalFieldDescription COUNT_FIELD = new KafkaInternalFieldDescription("_count", BigintType.BIGINT);
    public static final KafkaInternalFieldDescription OFFSET_FIELD = new KafkaInternalFieldDescription("_offset", BigintType.BIGINT);
    public static final KafkaInternalFieldDescription MESSAGE_LEN_FIELD = new KafkaInternalFieldDescription("_message_len", BigintType.BIGINT);

    public static Set<KafkaInternalFieldDescription> getInternalFields()
    {
        return ImmutableSet.of(MESSAGE_FIELD, CORRUPTED_FIELD, PARTITION_ID_FIELD, SEGMENT_START_FIELD, SEGMENT_END_FIELD, COUNT_FIELD, OFFSET_FIELD, MESSAGE_LEN_FIELD);
    }

    private final String name;
    private final Type type;

    private KafkaInternalFieldDescription(
            String name,
            Type type)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = checkNotNull(type, "type is null");
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    KafkaColumnHandle getColumnHandle(String connectorId, int index)
    {
        return new KafkaColumnHandle(connectorId,
                index,
                getName(),
                getType(),
                null,
                null,
                null,
                true,
                true);
    }

    ColumnMetadata getColumnMetadata(int index)
    {
        return new ColumnMetadata(name, type, index, true);
    }

    public KafkaInternalFieldValueProvider forBooleanValue(boolean value)
    {
        return new BooleanKafkaFieldValueProvider(value);
    }

    public KafkaInternalFieldValueProvider forLongValue(long value)
    {
        return new LongKafkaFieldValueProvider(value);
    }

    public KafkaInternalFieldValueProvider forByteValue(byte[] value)
    {
        return new BytesKafkaFieldValueProvider(value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KafkaInternalFieldDescription other = (KafkaInternalFieldDescription) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
    }

    public class BooleanKafkaFieldValueProvider
            extends KafkaInternalFieldValueProvider
    {
        private final boolean value;

        private BooleanKafkaFieldValueProvider(boolean value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getName().equals(name);
        }

        @Override
        public boolean getBoolean()
        {
            return value;
        }

        @Override
        public boolean isNull()
        {
            return false;
        }
    }

    public class LongKafkaFieldValueProvider
            extends KafkaInternalFieldValueProvider
    {
        private final long value;

        private LongKafkaFieldValueProvider(long value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getName().equals(name);
        }

        @Override
        public long getLong()
        {
            return value;
        }

        @Override
        public boolean isNull()
        {
            return false;
        }
    }

    public class BytesKafkaFieldValueProvider
            extends KafkaInternalFieldValueProvider
    {
        private final byte[] value;

        private BytesKafkaFieldValueProvider(byte[] value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(KafkaColumnHandle columnHandle)
        {
            return columnHandle.getName().equals(name);
        }

        @Override
        public Slice getSlice()
        {
            return isNull() ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(value);
        }

        @Override
        public boolean isNull()
        {
            return value == null || value.length == 0;
        }
    }
}
