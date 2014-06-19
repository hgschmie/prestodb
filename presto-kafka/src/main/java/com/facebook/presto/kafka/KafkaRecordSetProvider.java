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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;

public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger LOGGER = Logger.get(KafkaRecordSetProvider.class);

    private final String connectorId;
    private final KafkaHandleResolver handleResolver;

    @Inject
    public KafkaRecordSetProvider(@Named("connectorId") String connectorId,
                                  KafkaHandleResolver handleResolver)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        LOGGER.info("getRecordSet(%s, %s)", split, columns);

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ConnectorColumnHandle column : columns) {
            KafkaColumnHandle kch = (KafkaColumnHandle) column;
            types.add(kch.getColumnType());
        }

        final List<Type> columnTypes = types.build();

        return new RecordSet() {
            @Override
            public List<Type> getColumnTypes()
            {
                return columnTypes;
            }

            @Override
            public RecordCursor cursor()
            {
                return new RecordCursor() {
                    @Override
                    public long getTotalBytes()
                    {
                        return 0;
                    }

                    @Override
                    public long getCompletedBytes()
                    {
                        return 0;
                    }

                    @Override
                    public long getReadTimeNanos()
                    {
                        return 0;
                    }

                    @Override
                    public Type getType(int field)
                    {
                        return columnTypes.get(field);
                    }

                    @Override
                    public boolean advanceNextPosition()
                    {
                        return false;
                    }

                    @Override
                    public boolean getBoolean(int field)
                    {
                        return false;
                    }

                    @Override
                    public long getLong(int field)
                    {
                        return 0;
                    }

                    @Override
                    public double getDouble(int field)
                    {
                        return 0.0;
                    }

                    @Override
                    public Slice getSlice(int field)
                    {
                        return null;
                    }

                    @Override
                    public boolean isNull(int field)
                    {
                        return true;
                    }

                    @Override
                    public void close()
                    {
                    }
                };
            }

        };
    }
}
