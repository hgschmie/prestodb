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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import kafka.javaapi.FetchResponse;
import kafka.message.MessageAndOffset;

public class KafkaRecordCursor
        implements RecordCursor
{
    private static final Logger LOGGER = Logger.get(KafkaRecordCursor.class);

    private final List<KafkaColumnHandle> columnHandles;
    private final FetchResponse fetchResponse;

    private final Iterator<String> lines;
    private final long totalBytes;

    KafkaRecordCursor(KafkaSplit split, List<KafkaColumnHandle> columnHandles, FetchResponse fetchResponse)
    {
        LOGGER.info("KafkaRecordCursor(%s, %s, %s)", split, columnHandles, fetchResponse);
        this.columnHandles = columnHandles;
        this.fetchResponse = fetchResponse;

        long totalBytes = 0;

        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(split.getTopicName(), split.getPartitionId())) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < split.getStart()) {
                continue;
            }

            ByteBuffer payload = messageAndOffset.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            totalBytes += payload.limit();
            String msg = new String(bytes, StandardCharsets.UTF_8);
            LOGGER.info("Message is : %s (%d chars)", msg.substring(0, 10), msg.length());
            builder.add(msg);
        }

        this.lines = builder.build().iterator();
        this.totalBytes = totalBytes;

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
        if (!lines.hasNext()) {
            return false;
        }
        String line = lines.next();

        return true;
    }

    private String getFieldValue(int field)
    {
        return "";
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return true;
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return 0L;
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return 0.0d;
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
