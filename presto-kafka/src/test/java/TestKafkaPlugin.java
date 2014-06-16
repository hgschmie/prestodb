import com.facebook.presto.kafka.KafkaPlugin;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestKafkaPlugin
{
    private static final TypeManager DUMMY_TYPE_MANAGER = new TypeManager()
    {
        @Override
        public Type getType(String typeName)
        {
            return null;
        }
    };

    @Test
    public ConnectorFactory testConnectorExists()
    {
        KafkaPlugin plugin = new KafkaPlugin();
        plugin.setTypeManager(DUMMY_TYPE_MANAGER);

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
        Connector c = factory.create("test-connector", ImmutableMap.<String, String>of("node.environment", "test"));
        assertNotNull(c);
    }
}
