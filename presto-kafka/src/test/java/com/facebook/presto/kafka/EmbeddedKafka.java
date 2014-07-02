package com.facebook.presto.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.Closeable;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

public class EmbeddedKafka
        implements Closeable
{
    private final int port;
    private final File kafkaDataDir;
    private final KafkaServerStartable kafka;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public EmbeddedKafka(int port, int zookeeperPort)
    {
        this.port = port;
        this.kafkaDataDir = Files.createTempDir();

        Properties props = new Properties();

        props.putAll(ImmutableMap.<String, String>builder()
                .put("broker.id", "0")
                .put("host.name", "127.0.0.1")
                .put("num.partitions", "2")
                .put("log.flush.interval.messages", "10000")
                .put("log.flush.interval.ms", "1000")
                .put("log.retention.minutes", "60")
                .put("log.segment.bytes", "1048576")
                .put("zookeeper.connection.timeout.ms", "1000000")

                .put("port", Integer.toString(port))
                .put("log.dirs", kafkaDataDir.getAbsolutePath())
                .put("zookeeper.connect", "localhost:" + zookeeperPort)
                .build());

        KafkaConfig config = new KafkaConfig(props);
        this.kafka = new KafkaServerStartable(config);
    }

    public void start()
    {
        if (!started.getAndSet(true)) {
            kafka.startup();
        }
    }

    @Override
    public void close()
    {
        if (started.get() && !stopped.getAndSet(true)) {
            kafka.shutdown();
            kafka.awaitShutdown();
        }
    }

    public String getConnectString()
    {
        return "localhost:" + Integer.toString(port);
    }

    public void cleanup()
    {
        checkState(stopped.get(), "not stopped");

        for (File file : Files.fileTreeTraverser().postOrderTraversal(kafkaDataDir)) {
            file.delete();
        }
    }
}
