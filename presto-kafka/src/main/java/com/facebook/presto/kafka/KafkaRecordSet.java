/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka;

import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger LOG = Logger.get(KafkaRecordSet.class);

    private static final int KAFKA_READ_BUFFER_SIZE = 100_000;

    private final KafkaSplit split;
    private final KafkaSimpleConsumerManager consumerManager;

    private final KafkaRowDecoder rowDecoder;
    private final Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final Set<KafkaInternalFieldValueProvider> globalInternalFieldValueProviders;

    KafkaRecordSet(KafkaSplit split,
            KafkaSimpleConsumerManager consumerManager,
            List<KafkaColumnHandle> columnHandles,
            KafkaRowDecoder rowDecoder,
            Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders)
    {
        this.split = checkNotNull(split, "split is null");

        this.globalInternalFieldValueProviders = ImmutableSet.<KafkaInternalFieldValueProvider>of(
                KafkaInternalFieldDescription.PARTITION_ID_FIELD.forLongValue(split.getPartitionId()),
                KafkaInternalFieldDescription.SEGMENT_START_FIELD.forLongValue(split.getStart()),
                KafkaInternalFieldDescription.SEGMENT_END_FIELD.forLongValue(split.getEnd()));

        this.consumerManager = checkNotNull(consumerManager, "consumerManager is null");

        this.rowDecoder = checkNotNull(rowDecoder, "rowDecoder is null");
        this.fieldDecoders = checkNotNull(fieldDecoders, "fieldDecoders is null");

        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (KafkaColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KafkaRecordCursor();
    }

    public class KafkaRecordCursor
            implements RecordCursor
    {
        private long totalBytes = 0;
        private long totalMessages = 0;
        private long cursorOffset = split.getStart();
        private Iterator<MessageAndOffset> messageAndOffsetIterator;
        private final AtomicBoolean reported = new AtomicBoolean();

        private KafkaRow currentRow;

        KafkaRecordCursor()
        {
        }

        @Override
        public long getTotalBytes()
        {
            return totalBytes;
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (true) {
                if (cursorOffset >= split.getEnd()) {
                    return endOfData(); // Split end is exclusive.
                }
                // Create a fetch request
                openFetchRequest();

                while (messageAndOffsetIterator.hasNext()) {
                    MessageAndOffset currentMessageAndOffset = messageAndOffsetIterator.next();
                    long messageOffset = currentMessageAndOffset.offset();

                    if (messageOffset >= split.getEnd()) {
                        return endOfData(); // Past our split end. Bail.
                    }

                    if (messageOffset >= cursorOffset) {
                        return nextRow(currentMessageAndOffset);
                    }
                }
                messageAndOffsetIterator = null;
            }
        }

        private boolean endOfData()
        {
            if (!reported.getAndSet(true)) {
                LOG.debug("Found a total of %d messages with %d bytes (%d messages expected). Last Offset: %d (%d, %d)", totalMessages, totalBytes,
                        split.getEnd() - split.getStart(), cursorOffset, split.getStart(), split.getEnd());
            }
            return false;
        }

        private boolean nextRow(MessageAndOffset messageAndOffset)
        {
            cursorOffset = messageAndOffset.offset() + 1; // Cursor now points to the next message.
            totalBytes += messageAndOffset.message().payloadSize();
            totalMessages++;

            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] currentRow = new byte[payload.limit()];
            payload.get(currentRow);

            Set<KafkaInternalFieldValueProvider> internalFieldValueProviders = ImmutableSet.<KafkaInternalFieldValueProvider>builder()
                    .addAll(globalInternalFieldValueProviders)
                    .add(KafkaInternalFieldDescription.COUNT_FIELD.forLongValue(totalMessages))
                    .add(KafkaInternalFieldDescription.OFFSET_FIELD.forLongValue(messageAndOffset.offset()))
                    .add(KafkaInternalFieldDescription.MESSAGE_FIELD.forByteValue(currentRow))
                    .add(KafkaInternalFieldDescription.MESSAGE_LEN_FIELD.forLongValue(currentRow.length))
                    .build();

            this.currentRow = rowDecoder.decodeRow(currentRow, columnHandles, fieldDecoders, internalFieldValueProviders);

            return true; // Advanced successfully.
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, boolean.class);
            return currentRow.getBoolean(field);
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, long.class);
            return currentRow.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, double.class);
            return currentRow.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, Slice.class);
            return currentRow.getSlice(field);
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            return currentRow.isNull(field);
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
        }

        private void openFetchRequest()
        {
            if (messageAndOffsetIterator == null) {
                //                 LOG.info("Fetching %d bytes from offset %d (%d - %d). %d messages read so far", KAFKA_READ_BUFFER_SIZE, cursorOffset, split.getStart(), split.getEnd(), totalMessages);
                FetchRequest req = new FetchRequestBuilder()
                        .clientId("presto-worker-" + Thread.currentThread().getName())
                        .addFetch(split.getTopicName(), split.getPartitionId(), cursorOffset, KAFKA_READ_BUFFER_SIZE)
                        .build();

                // TODO - this should look at the actual node this is running on and prefer
                // that copy if running locally. - look into NodeInfo
                SimpleConsumer consumer = consumerManager.getConsumer(split.getNodes().get(0));

                FetchResponse fetchResponse = consumer.fetch(req);
                if (fetchResponse.hasError()) {
                    // TODO - this should probably do some actual error handling...
                    LOG.warn("Fetch response has error: %s", fetchResponse.errorCode(split.getTopicName(), split.getPartitionId()));
                    throw new IllegalStateException("Kafka split error!");
                }

                messageAndOffsetIterator = fetchResponse.messageSet(split.getTopicName(), split.getPartitionId()).iterator();
            }
        }
    }
}
