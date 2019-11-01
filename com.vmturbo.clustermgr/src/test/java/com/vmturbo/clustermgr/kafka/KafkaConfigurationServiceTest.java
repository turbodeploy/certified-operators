package com.vmturbo.clustermgr.kafka;

import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.clustermgr.kafka.KafkaConfigurationService.KafkaConfiguration;
import com.vmturbo.clustermgr.kafka.KafkaConfigurationService.TopicConfiguration;

/**
 * Tests methods on the KafkaConfigurationService. Although it's not testing the service itself.
 */
public class KafkaConfigurationServiceTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private KafkaConfigurationService createDefaultConfigurationService() {
        return new KafkaConfigurationService("localhost:9093",
                KafkaConfigurationServiceConfig.DEFAULT_CONFIG_MAX_RETRY_TIME_SECS,
                KafkaConfigurationServiceConfig.DEFAULT_CONFIG_RETRY_DELAY_MS);
    }

    @Test
    public void testKafkaConfigurationLoad() {
        KafkaConfigurationService kafkaConfigurationService = createDefaultConfigurationService();

        String testConfigFile = "/kafka-test-config.yml";
        KafkaConfiguration config = kafkaConfigurationService.readKafkaConfiguration(testConfigFile);

        Assert.assertEquals("Two topics should be loaded",2, config.getTopics().size());
        List<TopicConfiguration> topicConfigs = config.getTopics();
        // validate topic 1
        TopicConfiguration topic1 = topicConfigs.get(0);
        Assert.assertEquals("topic 1 is test-topic", "test-topic", topic1.getTopic());
        // validate the topic 1 properties
        Assert.assertFalse("preallocate should be false", (Boolean) topic1.getProperties().get("preallocate"));
        Assert.assertEquals("message.timestamp.type should be 'CreateTime'", "CreateTime", topic1.getProperties().get("message.timestamp.type"));
        Assert.assertEquals("max.message.bytes should be 67108864", 67108864, topic1.getProperties().get("max.message.bytes"));
        // validate topic 2
        TopicConfiguration topic2 = topicConfigs.get(1);
        Assert.assertEquals("topic 2 is test-topic2", "test-topic2", topic2.getTopic());
        Assert.assertNull("topic 2 has no properties", topic2.getProperties());
    }

    @Test
    public void testKafkaConfigurationServiceEmptyUrl() {
        // test bad URL's
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("bootstrapServers must have a value.");
        KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService("",1,1);
    }

    @Test
    public void testKafkaConfigurationServiceIllegalTimeout() {
        // test timeout less than zero
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Configuration max retry time cannot be less than zero.");
        KafkaConfigurationService kafkaConfigurationService = new KafkaConfigurationService("blah",-1,1);
    }

}
