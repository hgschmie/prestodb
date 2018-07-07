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
package com.facebook.presto.connector.system;

import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.ColumnName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Inject;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.ColumnName.*;
import static com.facebook.presto.spi.SystemTable.Distribution.ALL_NODES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class TaskSystemTable
        implements SystemTable
{
    public static final SchemaTableName TASK_TABLE_NAME = new SchemaTableName("runtime", "tasks");

    public static final ConnectorTableMetadata TASK_TABLE = tableMetadataBuilder(TASK_TABLE_NAME)
            .column(createColumnName("node_id"), createUnboundedVarcharType())

            .column(createColumnName("task_id"), createUnboundedVarcharType())
            .column(createColumnName("stage_id"), createUnboundedVarcharType())
            .column(createColumnName("query_id"), createUnboundedVarcharType())
            .column(createColumnName("state"), createUnboundedVarcharType())

            .column(createColumnName("splits"), BIGINT)
            .column(createColumnName("queued_splits"), BIGINT)
            .column(createColumnName("running_splits"), BIGINT)
            .column(createColumnName("completed_splits"), BIGINT)

            .column(createColumnName("split_scheduled_time_ms"), BIGINT)
            .column(createColumnName("split_cpu_time_ms"), BIGINT)
            .column(createColumnName("split_user_time_ms"), BIGINT)
            .column(createColumnName("split_blocked_time_ms"), BIGINT)

            .column(createColumnName("raw_input_bytes"), BIGINT)
            .column(createColumnName("raw_input_rows"), BIGINT)

            .column(createColumnName("processed_input_bytes"), BIGINT)
            .column(createColumnName("processed_input_rows"), BIGINT)

            .column(createColumnName("output_bytes"), BIGINT)
            .column(createColumnName("output_rows"), BIGINT)

            .column(createColumnName("physical_written_bytes"), BIGINT)

            .column(createColumnName("created"), TIMESTAMP)
            .column(createColumnName("start"), TIMESTAMP)
            .column(createColumnName("last_heartbeat"), TIMESTAMP)
            .column(createColumnName("end"), TIMESTAMP)
            .build();

    private final TaskManager taskManager;
    private final String nodeId;

    @Inject
    public TaskSystemTable(TaskManager taskManager, NodeInfo nodeInfo)
    {
        this.taskManager = taskManager;
        this.nodeId = nodeInfo.getNodeId();
    }

    @Override
    public Distribution getDistribution()
    {
        return ALL_NODES;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TASK_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(TASK_TABLE);
        for (TaskInfo taskInfo : taskManager.getAllTaskInfo()) {
            TaskStats stats = taskInfo.getStats();
            TaskStatus taskStatus = taskInfo.getTaskStatus();
            table.addRow(
                    nodeId,

                    taskStatus.getTaskId().toString(),
                    taskStatus.getTaskId().getStageId().toString(),
                    taskStatus.getTaskId().getQueryId().toString(),
                    taskStatus.getState().toString(),

                    (long) stats.getTotalDrivers(),
                    (long) stats.getQueuedDrivers(),
                    (long) stats.getRunningDrivers(),
                    (long) stats.getCompletedDrivers(),

                    toMillis(stats.getTotalScheduledTime()),
                    toMillis(stats.getTotalCpuTime()),
                    toMillis(stats.getTotalUserTime()),
                    toMillis(stats.getTotalBlockedTime()),

                    toBytes(stats.getRawInputDataSize()),
                    stats.getRawInputPositions(),

                    toBytes(stats.getProcessedInputDataSize()),
                    stats.getProcessedInputPositions(),

                    toBytes(stats.getOutputDataSize()),
                    stats.getOutputPositions(),

                    toBytes(stats.getPhysicalWrittenDataSize()),

                    toTimeStamp(stats.getCreateTime()),
                    toTimeStamp(stats.getFirstStartTime()),
                    toTimeStamp(taskInfo.getLastHeartbeat()),
                    toTimeStamp(stats.getEndTime()));
        }
        return table.build().cursor();
    }

    private static Long toMillis(Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    private static Long toBytes(DataSize dataSize)
    {
        if (dataSize == null) {
            return null;
        }
        return dataSize.toBytes();
    }

    private static Long toTimeStamp(DateTime dateTime)
    {
        if (dateTime == null) {
            return null;
        }
        return dateTime.getMillis();
    }
}
