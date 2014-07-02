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
    /**
     * <tt>_message</tt> - Represents the full topic as a text column. Format is UTF-8 which may be wrong for some topics. TODO - make charset configureable.
     */
    public static final KafkaInternalFieldDescription MESSAGE_FIELD = new KafkaInternalFieldDescription("_message", VarcharType.VARCHAR);

    /**
     * <tt>_corrupt</tt> - True if the row converter could not read the a message. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    public static final KafkaInternalFieldDescription CORRUPT_FIELD = new KafkaInternalFieldDescription("_corrupt", BooleanType.BOOLEAN);

    /**
     * <tt>_partition_id</tt> - Kafka partition id.
     */
    public static final KafkaInternalFieldDescription PARTITION_ID_FIELD = new KafkaInternalFieldDescription("_partition_id", BigintType.BIGINT);

    /**
     * <tt>_segment_start</tt> - Kafka start offset for the segment which contains the current message. This is per-partition.
     */
    public static final KafkaInternalFieldDescription SEGMENT_START_FIELD = new KafkaInternalFieldDescription("_segment_start", BigintType.BIGINT);

    /**
     * <tt>_segment_end</tt> - Kafka end offset for the segment which contains the current message. This is per-partition. The end offset is the first offset that is *not* in the segment.
     */
    public static final KafkaInternalFieldDescription SEGMENT_END_FIELD = new KafkaInternalFieldDescription("_segment_end", BigintType.BIGINT);

    /**
     * <tt>_segment_count</tt> - Running count of messages in a segment.
     */
    public static final KafkaInternalFieldDescription SEGMENT_COUNT_FIELD = new KafkaInternalFieldDescription("_segment_count", BigintType.BIGINT);

    /**
     * <tt>_partition_offset</tt> - The current offset of the message in the partition.
     */
    public static final KafkaInternalFieldDescription PARTITION_OFFSET_FIELD = new KafkaInternalFieldDescription("_partition_offset", BigintType.BIGINT);

    /**
     * <tt>_message_length</tt> - length in bytes of the message.
     */
    public static final KafkaInternalFieldDescription MESSAGE_LENGTH_FIELD = new KafkaInternalFieldDescription("_message_length", BigintType.BIGINT);

    public static Set<KafkaInternalFieldDescription> getInternalFields()
    {
        return ImmutableSet.of(MESSAGE_FIELD, CORRUPT_FIELD, PARTITION_ID_FIELD, SEGMENT_START_FIELD, SEGMENT_END_FIELD, SEGMENT_COUNT_FIELD, PARTITION_OFFSET_FIELD, MESSAGE_LENGTH_FIELD);
    }

    private final String name;
    private final Type type;

    KafkaInternalFieldDescription(
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

    KafkaColumnHandle getColumnHandle(String connectorId, int index, boolean hidden)
    {
        return new KafkaColumnHandle(connectorId,
                index,
                getName(),
                getType(),
                null,
                null,
                null,
                hidden,
                true);
    }

    ColumnMetadata getColumnMetadata(int index, boolean hidden)
    {
        return new ColumnMetadata(name, type, index, false, hidden);
    }

    public KafkaFieldValueProvider forBooleanValue(boolean value)
    {
        return new BooleanKafkaFieldValueProvider(value);
    }

    public KafkaFieldValueProvider forLongValue(long value)
    {
        return new LongKafkaFieldValueProvider(value);
    }

    public KafkaFieldValueProvider forByteValue(byte[] value)
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
            extends KafkaFieldValueProvider
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
            extends KafkaFieldValueProvider
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
            extends KafkaFieldValueProvider
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
