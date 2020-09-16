package com.vmturbo.clustermgr.kafka;

import static com.vmturbo.clustermgr.kafka.KafkaConfigurationService.KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.clustermgr.kafka.KafkaConfigurationService.KafkaConfiguration;
import com.vmturbo.clustermgr.kafka.KafkaConfigurationService.TopicConfiguration;
import com.vmturbo.components.common.config.PropertiesConfigSource;

/**
 * Tests methods on the KafkaConfigurationService. Although it's not testing the service itself.
 */
public class KafkaConfigurationServiceTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private PropertiesConfigSource configSource = createDefaultConfigurationSource();

    private PropertiesConfigSource createDefaultConfigurationSource() {
        PropertiesConfigSource configSource = new PropertiesConfigSource();
        configSource.setProperty("kafkaServers", "localhost:9093");
        // disable the async config load that starts when instantiating the component by setting the
        // config path to empty. We will be testing the config load logic explicitly in our tests here.
        configSource.setProperty("kafkaConfigFile", "");
        return configSource;
    }

    @Test
    public void testKafkaConfigurationLoad() {
        KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService(configSource);

        String testConfigFile = "src/test/resources/kafka-test-config.yml";
        KafkaConfiguration config = kafkaConfigurationService.readKafkaConfiguration(testConfigFile);

        assertEquals("All topics should be loaded",4, config.getTopics().size());
        List<TopicConfiguration> topicConfigs = config.getTopics();
        // validate topic 1
        TopicConfiguration topic1 = topicConfigs.get(0);
        assertEquals("topic 1 is test-topic", "test-topic", topic1.getTopic());
        // validate the topic 1 properties
        Assert.assertFalse("preallocate should be false", (Boolean) topic1.getProperties().get("preallocate"));
        assertEquals("message.timestamp.type should be 'CreateTime'", "CreateTime", topic1.getProperties().get("message.timestamp.type"));
        assertEquals("max.message.bytes should be 67108864", 67108864, topic1.getProperties().get("max.message.bytes"));
        // validate topic 2 has no props and default replication factor/partitions
        TopicConfiguration topic2 = topicConfigs.get(1);
        assertEquals("topic 2 is test-topic2", "test-topic2", topic2.getTopic());
        Assert.assertNull("topic 2 has no properties", topic2.getProperties());
        assertEquals("topic 2 has replication factor 1", 1, topic2.getReplicationFactor());
        assertEquals("topic 2 has 1 partitions", 1, topic2.getPartitions());
        // validate topic 3 has non-default partition/replication factor
        TopicConfiguration topic3 = topicConfigs.get(2);
        assertEquals("topic 3 is test-partitions", "test-partitions", topic3.getTopic());
        assertEquals("topic 3 has replication factor 2", 2, topic3.getReplicationFactor());
        assertEquals("topic 3 has 3 partitions", 3, topic3.getPartitions());
        Assert.assertNull("topic 3 has no properties", topic3.getProperties());
    }

    /**
     * Verify that the topics loaded will be prefixed with the namespace, when a namespace is defined.
     */
    @Test
    public void testKafkaConfigurationLoadWithNamespace() {
        String namespace = "namespace";
        String namespacePrefix = namespace + ".";
        configSource.setProperty("kafkaNamespace", namespace);
        KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService(configSource);

        String testConfigFile = "src/test/resources/kafka-test-config.yml";
        KafkaConfiguration config = kafkaConfigurationService.readKafkaConfiguration(testConfigFile);

        // verify that both topics are prefixed correctly
        List<TopicConfiguration> topicConfigs = config.getTopics();
        // validate topic 1
        TopicConfiguration topic1 = topicConfigs.get(0);
        assertEquals("topic 1 is namespace-test-topic", namespacePrefix + "test-topic", topic1.getTopic());
        // validate the topic 1 properties
        Assert.assertFalse("preallocate should be false", (Boolean) topic1.getProperties().get("preallocate"));
        assertEquals("message.timestamp.type should be 'CreateTime'", "CreateTime", topic1.getProperties().get("message.timestamp.type"));
        assertEquals("max.message.bytes should be 67108864", 67108864, topic1.getProperties().get("max.message.bytes"));
        // validate topic 2
        TopicConfiguration topic2 = topicConfigs.get(1);
        assertEquals("topic 2 is namespace-test-topic2", namespacePrefix + "test-topic2", topic2.getTopic());
        Assert.assertNull("topic 2 has no properties", topic2.getProperties());
    }

    /**
     * Test that the topic configurations are created as expected and account for defaults.
     */
    @Test
    public void testKafkaConfigurationTopicProperties() {
        KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService(configSource);

        String testConfigFile = "src/test/resources/kafka-test-config.yml";
        KafkaConfiguration config = kafkaConfigurationService.readKafkaConfiguration(testConfigFile);

        // get the kafka topic configs
        Map<ConfigResource, Config> configMap = kafkaConfigurationService.getKafkaTopicConfigurations(config);

        // validate test-topic properties
        assertEquals(Boolean.FALSE.toString(), getTopicConfigProperty(configMap, "test-topic", "preallocate"));
        assertEquals("CreateTime", getTopicConfigProperty(configMap, "test-topic", "message.timestamp.type"));
        assertEquals("67108864", getTopicConfigProperty(configMap, "test-topic", "max.message.bytes"));
        assertEquals(Boolean.TRUE.toString(), getTopicConfigProperty(configMap, "test-topic", KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE));
        // validate that test-topic2 has only defaults
        assertEquals(Boolean.TRUE.toString(), getTopicConfigProperty(configMap, "test-topic2", KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE));
        // validate that test-partitions also only has defaults
        assertEquals(Boolean.TRUE.toString(), getTopicConfigProperty(configMap, "test-partitions", KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE));
        // validate topic test-unclean-leader-election-disabled has unclean leader election ENABLED, as it was overridden at the topic level.
        assertEquals(Boolean.FALSE.toString(), getTopicConfigProperty(configMap, "test-unclean-leader-disabled", KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE));
    }

    // utility for getting a config property value from a kafka config map. We assume each value
    // actually exists.
    private String getTopicConfigProperty(Map<ConfigResource, Config> configMap, String topicName, String propertyName) {
        // create the key to look up
        ConfigResource resource = new ConfigResource(Type.TOPIC, topicName);
        assertTrue(configMap.containsKey(resource));
        Config config = configMap.get(resource);
        ConfigEntry entry = config.get(propertyName);
        assertNotNull(entry);
        return entry.value();
    }

    @Test
    public void testKafkaConfigurationServiceEmptyUrl() {
        // test bad URL's
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("bootstrapServers must have a value.");
        configSource.setProperty("kafkaServers", "");
        KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService(configSource);
    }

    @Test
    public void testKafkaConfigurationServiceIllegalTimeout() {
        // test timeout less than zero
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("kafka.config.max.retry.time.secs cannot be less than zero.");
        configSource.setProperty("kafka.config.max.retry.time.secs", -1);
        KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService(configSource);
    }

}
