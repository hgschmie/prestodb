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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestKafkaPlugin
{
    private static final TypeManager DUMMY_TYPE_MANAGER = new TypeManager()
    {
        @Override
        public Type getType(String typeName)
        {
            return null;
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }
    };

    private static final NodeManager DUMMY_NODE_MANAGER = new NodeManager()
    {
        @Override
        public Set<Node> getActiveNodes()
        {
            return ImmutableSet.of(LOCAL_NODE);
        }

        @Override
        public Set<Node> getActiveDatasourceNodes(String datasourceName)
        {
            return ImmutableSet.of(LOCAL_NODE);
        }

        @Override
        public Node getCurrentNode()
        {
            return LOCAL_NODE;
        }
    };

    private static final Node LOCAL_NODE = new Node()
    {
        @Override
        public HostAddress getHostAndPort()
        {
            return HostAddress.fromParts("localhost", 8080);
        }

        @Override
        public URI getHttpUri()
        {
            return URI.create("http://localhost:8080/");
        }

        @Override
        public String getNodeIdentifier()
        {
            return UUID.randomUUID().toString();
        }
    };

    @Test
    public ConnectorFactory testConnectorExists()
    {
        KafkaPlugin plugin = new KafkaPlugin();
        plugin.setTypeManager(DUMMY_TYPE_MANAGER);
        plugin.setNodeManager(DUMMY_NODE_MANAGER);

        List<ConnectorFactory> factories = plugin.getServices(ConnectorFactory.class);
        assertNotNull(factories);
        assertEquals(factories.size(), 1);
        ConnectorFactory factory = factories.get(0);
        assertNotNull(factory);
        return factory;
    }

    @Test
    public void testSpinup()
    {
        ConnectorFactory factory = testConnectorExists();
        Connector c = factory.create("test-connector", ImmutableMap.<String, String>of("kafka.table-names", "test", "kafka.nodes", "localhost:9092"));
        assertNotNull(c);
    }
}
