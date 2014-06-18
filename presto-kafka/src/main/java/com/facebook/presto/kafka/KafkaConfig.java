package com.facebook.presto.kafka;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

public class KafkaConfig
{
    /**
     * Zookeeper URL for Kafka
     */
    private String zookeeperUrl = "zk://localhost:2181/";

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

    @Config("plugin.config-dir")
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
    public String getZookeeperUrl()
    {
        return zookeeperUrl;
    }

    @Config("kafka.zookeeper-url")
    public KafkaConfig setZookeeperUrl(String zookeeperUrl)
    {
        this.zookeeperUrl = zookeeperUrl;
        return this;
    }
}
