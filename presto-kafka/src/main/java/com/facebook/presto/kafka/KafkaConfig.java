package com.facebook.presto.kafka;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class KafkaConfig
{
    private String zookeeperUrl = "zk://localhost:2181/";

    @NotNull
    public String getZookeeperUrl()
    {
        return zookeeperUrl;
    }

    @Config("kafka.zookeeper-url")
    public void setZookeeperUrl(String zookeeperUrl)
    {
        this.zookeeperUrl = zookeeperUrl;
    }
}
