package com.facebook.presto.kafka;

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

public class KafkaConfig
{
    /**
     * Seed nodes for Kafka cluster. At least one must exist.
     */
    private Set<HostAddress> nodes = null;

    /**
     * Timeout to connect to Kafka.
     */
    private Duration kafkaConnectTimeout = Duration.valueOf("10s");

    /**
     * Buffer size for connecting to Kafka.
     */
    private DataSize kafkaBufferSize = new DataSize(64, Unit.KILOBYTE);

    /**
     * The schema name to use in the connector.
     */
    private String schemaName = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Kafka topic / tables.
     */
    private File tableDescriptionDir = new File("etc/kafka/");

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kafka.table-description-dir")
    public KafkaConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kafka.table-names")
    public KafkaConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getSchemaName()
    {
        return schemaName;
    }

    @Config("kafka.schema-name")
    public KafkaConfig setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

    @NotNull
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("kafka.nodes")
    public KafkaConfig setNodes(String nodes)
    {
        this.nodes = ImmutableSet.copyOf(Iterables.transform(Splitter.on(',').omitEmptyStrings().trimResults().split(nodes), new Function<String, HostAddress>()
        {
            @Override
            public HostAddress apply(@Nonnull String value)
            {
                return HostAddress.fromString(value);
            }
        }));

        return this;
    }

    @MinDuration("1s")
    public Duration getKafkaConnectTimeout()
    {
        return kafkaConnectTimeout;
    }

    @Config("kafka.connect-timeout")
    public void setKafkaConnectTimeout(String kafkaConnectTimeout)
    {
        this.kafkaConnectTimeout = Duration.valueOf(kafkaConnectTimeout);
    }

    public DataSize getKafkaBufferSize()
    {
        return kafkaBufferSize;
    }

    @Config("kafka.buffer-size")
    public void setKafkaBufferSize(String kafkaBufferSize)
    {
        this.kafkaBufferSize = DataSize.valueOf(kafkaBufferSize);
    }
}
