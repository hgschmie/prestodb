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

import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import kafka.producer.KeyedMessage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import static com.facebook.presto.kafka.util.EmbeddedKafka.CloseableProducer;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "kafka", "default", UTC_KEY, Locale.ENGLISH, null, null);

    private static EmbeddedKafka embeddedKafka;

    private StandaloneQueryRunner queryRunner;
    private String topicName;

    @BeforeClass
    public static void startKafka()
            throws Exception
    {
        embeddedKafka = EmbeddedKafka.createEmbeddedKafka();
        embeddedKafka.start();
    }

    @AfterClass
    public static void stopKafka()
            throws Exception
    {
        embeddedKafka.close();
        embeddedKafka.cleanup();
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.topicName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        Properties topicProperties = new Properties();
        embeddedKafka.createTopics(2, 1, topicProperties, topicName);

        this.queryRunner = new StandaloneQueryRunner(SESSION);

        TestUtils.installKafkaPlugin(embeddedKafka, queryRunner,
                ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(TestUtils.createEmptyTopicDescription(topicName, new SchemaTableName("default", topicName)))
                        .build());
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        queryRunner.close();
    }

    private void createMessages(String topicName, int count)
    {
        try (CloseableProducer<Long, Object> producer = embeddedKafka.createProducer()) {
            for (long i = 0; i < count; i++) {
                KeyedMessage<Long, Object> msg = new KeyedMessage<Long, Object>(topicName, i, ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString()));
                producer.send(msg);
            }
        }
    }

    @Test
    public void testTopicExists()
            throws Exception
    {
        QualifiedTableName name = new QualifiedTableName("kafka", "default", topicName);
        Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(SESSION, name);
        assertTrue(handle.isPresent());
    }

    @Test
    public void testTopicHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + topicName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0)
                .build();

        assertEquals(result, expected);

        int count = 1000;
        createMessages(topicName, count);

        result = queryRunner.execute("SELECT count(1) from " + topicName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(count)
                .build();

        assertEquals(result, expected);
    }
}
