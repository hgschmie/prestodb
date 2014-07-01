package com.facebook.presto.kafka;

import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;
import io.airlift.log.Logging;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.facebook.presto.kafka.TestUtils.findUnusedPort;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestEmbeddedKafka
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "kafka", "default", UTC_KEY, Locale.ENGLISH, null, null);

    private final Closer closer = Closer.create();

    private EmbeddedZookeeper embeddedZookeeper;
    private EmbeddedKafka embeddedKafka;
    private LocalQueryRunner queryRunner;

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        Logging.initialize();

        int zkPort = findUnusedPort();
        this.embeddedZookeeper = closer.register(new EmbeddedZookeeper(zkPort));
        this.embeddedZookeeper.start();

        int kafkaPort = findUnusedPort();
        this.embeddedKafka = closer.register(new EmbeddedKafka(kafkaPort, zkPort));
        this.embeddedKafka.start();

        ZkClient zkClient = new ZkClient(embeddedZookeeper.getConnectString(), 30_000, 30_000, ZKStringSerializer$.MODULE$);
        AdminUtils.createTopic(zkClient, "test", 2, 1, new Properties());

        this.queryRunner = closer.register(new LocalQueryRunner(SESSION));
        // add Kafka
        InMemoryNodeManager nodeManager = queryRunner.getNodeManager();
        TypeRegistry typeManager = queryRunner.getTypeManager();
        Map<String, String> kafkaConfig = ImmutableMap.<String, String>of(
                "kafka.nodes", embeddedKafka.getConnectString(),
                "kafka.table-names", "test",
                "kafka.connect-timeout", "120s");

        queryRunner.createCatalog("kafka", new KafkaConnectorFactory(typeManager, nodeManager, ImmutableMap.<String, String>of()), kafkaConfig);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        closer.close();

        embeddedKafka.cleanup();
        embeddedZookeeper.cleanup();
    }

    @Test
    public void testTopicExists() throws Exception
    {
        QualifiedTableName name =  new QualifiedTableName("kafka", "default", "test");
        Optional<TableHandle> handle = queryRunner.getMetadata().getTableHandle(SESSION, name);
        assertTrue(handle.isPresent());
    }

    @Test
    public void testTopicHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) from test");

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0)
                .build();

        assertEquals(result, expected);

        Properties props = new Properties();
        props.putAll(ImmutableMap.<String, String>builder()
                .put("metadata.broker.list", embeddedKafka.getConnectString())
                .put("serializer.class", JsonEncoder.class.getName())
                .put("key.serializer.class", NumberEncoder.class.getName())
                .put("partitioner.class", NumberPartitioner.class.getName())
                .put("request.required.acks", "1")
                .build());

        ProducerConfig producerConfig = new ProducerConfig(props);
        Producer<Long, Object> producer = new Producer<>(producerConfig);

        for (int i = 0; i < 1000; i++) {
            KeyedMessage msg = new KeyedMessage("test", i, ImmutableMap.of("id", Integer.toString(i), "value", UUID.randomUUID().toString()));
            producer.send(msg);
        }

        producer.close();

        Thread.sleep(100L);

        result = queryRunner.execute("SELECT count(1) from test");

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1000)
                .build();

        assertEquals(result, expected);
    }
}
