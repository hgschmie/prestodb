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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger LOGGER = Logger.get(KafkaRecordSet.class);

    private static final int KAFKA_READ_BUFFER_SIZE = 100_000;

    private final KafkaSimpleConsumerManager consumerManager;
    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final Map<String, Type> mapping;
    private final KafkaSplit split;

    private final KafkaDecoder kafkaDecoder;

    KafkaRecordSet(KafkaDecoder kafkaDecoder,
            KafkaSplit split,
            KafkaSimpleConsumerManager consumerManager,
            List<KafkaColumnHandle> columnHandles)
    {
        this.kafkaDecoder = checkNotNull(kafkaDecoder, "kafkaDecoder is null");
        this.split = checkNotNull(split, "split is null");

        this.consumerManager = checkNotNull(consumerManager, "consumerManager is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, Type> mappingBuilder = ImmutableMap.builder();

        for (KafkaColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getColumnType());
            mappingBuilder.put(handle.getMapping(), handle.getColumnType());
        }

        this.columnTypes = typeBuilder.build();
        this.mapping = mappingBuilder.build();

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
            return columnHandles.get(field).getColumnType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            for (;; ) {
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
                LOGGER.debug("Found a total of %d messages with %d bytes (%d messages expected). Last Offset: %d", totalMessages, totalBytes,
                        split.getEnd() - split.getStart(), cursorOffset);
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
            this.currentRow = kafkaDecoder.decodeRow(currentRow, mapping);

            return true; // Advanced successfully.
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            KafkaColumnHandle columnHandle = columnHandles.get(field);

            checkFieldType(field, boolean.class);
            return currentRow.getBoolean(columnHandle.getMapping());
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            KafkaColumnHandle columnHandle = columnHandles.get(field);

            checkFieldType(field, long.class);
            return currentRow.getLong(columnHandle.getMapping());
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            KafkaColumnHandle columnHandle = columnHandles.get(field);

            checkFieldType(field, double.class);
            return currentRow.getDouble(columnHandle.getMapping());
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            KafkaColumnHandle columnHandle = columnHandles.get(field);

            checkFieldType(field, Slice.class);
            return currentRow.getSlice(columnHandle.getMapping());
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            KafkaColumnHandle columnHandle = columnHandles.get(field);

            return currentRow.isNull(columnHandle.getMapping());
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
                //                 LOGGER.info("Fetching %d bytes from offset %d (%d - %d). %d messages read so far", KAFKA_READ_BUFFER_SIZE, cursorOffset, split.getStart(), split.getEnd(), totalMessages);
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
                    LOGGER.warn("Fetch response has error: %s", fetchResponse.errorCode(split.getTopicName(), split.getPartitionId()));
                    throw new IllegalStateException("Kafka split error!");
                }

                messageAndOffsetIterator = fetchResponse.messageSet(split.getTopicName(), split.getPartitionId()).iterator();
            }
        }
    }
}
