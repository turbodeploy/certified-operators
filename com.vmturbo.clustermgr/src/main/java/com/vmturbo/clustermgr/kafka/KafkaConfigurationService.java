package com.vmturbo.clustermgr.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StringUtils;
import org.yaml.snakeyaml.Yaml;

import com.google.common.annotations.VisibleForTesting;

/**
 * Configures kafka with our chosen settings.
 * In the future, we will extend this to support partition counts and replication factors. In the
 * meantime, it will default to 1 partition, 1 replication factor.
 */
public class KafkaConfigurationService {
    private final Logger log = LogManager.getLogger();

    private static final int DEFAULT_TOPIC_PARTITION_COUNT = 1;
    private static final short DEFAULT_TOPIC_REPLICATION_FACTOR = 1;

    /**
     * the config service will retry configuration applications as long as "max retry time" hasn't
     * elapsed
     */
    private final int kafkaConfigMaxRetryTimeSecs;

    /**
     * amount of time between config retry attempts
     */
    private final int kafkaConfigRetryDelayMs;

    private final String kafkaBootstrapServers;

    private final String namespacePrefix;

    /**
     * Create a KafkaConfigurationService that will apply a topic configuration to the kafka
     * cluster at bootstrap servers, and use custom timeout/retry settings you provide.
     * @param bootstrapServers one or more kafka broker addresses to connect to.
     * @param kafkaConfigMaxRetryTimeSecs the amount of time the config service can start a retry within
     * @param configRetryDelayMs the number of milliseconds to wait between retry attempts
     */
    public KafkaConfigurationService(@Nonnull String bootstrapServers, int kafkaConfigMaxRetryTimeSecs, int configRetryDelayMs) {
        this(bootstrapServers, kafkaConfigMaxRetryTimeSecs, configRetryDelayMs, "");
    }

    /**
     * Create a KafkaConfigurationService that will apply a topic configuration to the kafka
     * cluster at bootstrap servers, and use custom timeout/retry settings you provide.
     * @param bootstrapServers one or more kafka broker addresses to connect to.
     * @param kafkaConfigMaxRetryTimeSecs the amount of time the config service can start a retry within
     * @param configRetryDelayMs the number of milliseconds to wait between retry attempts
     * @param namespacePrefix namespace of this XL deployment
     */
    public KafkaConfigurationService(@Nonnull String bootstrapServers,
                                     int kafkaConfigMaxRetryTimeSecs, int configRetryDelayMs,
                                     @Nonnull String namespacePrefix) {
        this.namespacePrefix = Objects.requireNonNull(namespacePrefix);

        if (StringUtils.isEmpty(bootstrapServers)) {
            throw new IllegalArgumentException("bootstrapServers must have a value.");
        }
        if (kafkaConfigMaxRetryTimeSecs < 0) {
            throw new IllegalArgumentException("Configuration max retry time cannot be less than zero.");
        }
        if (configRetryDelayMs < 0) {
            throw new IllegalArgumentException("Configuration retry delay cannot be less than zero.");
        }

        kafkaBootstrapServers = bootstrapServers;
        this.kafkaConfigMaxRetryTimeSecs = kafkaConfigMaxRetryTimeSecs;
        kafkaConfigRetryDelayMs = configRetryDelayMs;
    }

    /**
     * Load the kafka config yaml file and apply it to the kafka brokers. This method will keep trying
     * to apply the configuration until either successful, or the max retry time has elapsed.
     *
     * Note that regardless of the max retry time setting value, the underlying kafka AdminClient will
     * embed a number of connection retries of it's own until it can connect to the cluster. This
     * would time out after about five minutes if continuously unsuccessful. The time spent during
     * these Admin Client retries does count against the max retry time budget.
     *
     * @param configFile the path to a kafka config.yml to load
     * @throws TimeoutException if the configuration attempt doesn't succeed within the specified timeout
     * @throws InterruptedException if the configuration delay thread is interrupted
     */
    public void loadConfiguration(@Nonnull String configFile) throws TimeoutException, InterruptedException {
        // load the kafka configuration
        KafkaConfiguration kafkaConfig = readKafkaConfiguration(configFile);

        // create the topics and set the requested topic-specific properties on them
        log.info("Starting kafka configuration.");
        long startTime = System.currentTimeMillis();

        // keep trying until we succeed or hit the timeout threshold
        while (true) {
            try {
                applyKafkaConfiguration(kafkaConfig);
                log.info("Kafka configuration complete. Took {} ms",
                        System.currentTimeMillis() - startTime);
                return;
            } catch (ExecutionException | KafkaException ee) {
                // retryable
                log.warn("Error while applying kafka configuration.", ee);
            }
            // do we have time for another attempt?
            long timeElapsedMs = System.currentTimeMillis() - startTime;
            if (kafkaConfigRetryDelayMs + timeElapsedMs > (1000 * kafkaConfigMaxRetryTimeSecs)) {
                log.error("Kafka configuration failed -- timing out after {} secs", timeElapsedMs / 1000);
                throw new TimeoutException("Kafka configuration attempts exceeded allotted time of "
                        + kafkaConfigMaxRetryTimeSecs + " seconds");
            }

            // still have time -- sleep and then try again
            log.warn("Kafka configuration attempt unsuccessful - will try again in {} ms.",
                    kafkaConfigRetryDelayMs);
            Thread.sleep(kafkaConfigRetryDelayMs);
        }
    }

    /**
     * Create a kafka admin client. Keep trying until successful or the configMaxRetryTimeSecs
     * elapses. Note that if configMaxRetryTimeSecs is less than 30 seconds, we will fall back to
     * setting a 30 second timeout.
     * @param bootstrapServers the kafka brokers to connect the AdminClient to
     * @return a kafka admin client connected to the kafka servers requested
     */
    private AdminClient createKafkaAdminClient(@Nonnull String bootstrapServers) {
        Objects.requireNonNull(bootstrapServers);

        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        // Setting request.timeout.ms to match the config max retry time interval, if specified and
        // non-zero. Otherwise we will stick with the kafka default setting for this.
        if (kafkaConfigMaxRetryTimeSecs > 0) {
            // minimum of 10 seconds to give the admin client time to actually do stuff.
            props.put("request.timeout.ms", Math.max(10000, 1000 * kafkaConfigMaxRetryTimeSecs));
        }

        log.info("Creating kafka admin client with properties {}", props);

        return AdminClient.create(props);
    }

    /**
     * Read the kafka config .yml file from the specific location and return java object representation.
     * @param configFileRelativePath the path to a kafka config .yml file to load
     * @return a KafkaConfiguration object containing the topic configuration data
     */
    @VisibleForTesting
    KafkaConfiguration readKafkaConfiguration(@Nonnull String configFileRelativePath) {
        final KafkaConfiguration kafkaConfiguration;
        log.info("Loading kafka configuration from {}", configFileRelativePath);
        try (InputStream inputStream = Files.newInputStream(Paths.get(configFileRelativePath))) {
            if (inputStream == null) {
                throw new RuntimeException("Couldn't open input stream for " + configFileRelativePath);
            }
            kafkaConfiguration = new Yaml().loadAs(inputStream, KafkaConfiguration.class);
            log.debug("Kafka configuration loaded {} topics.", kafkaConfiguration.getTopics().size());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File " + configFileRelativePath + " Not Found.", e);
        } catch (IOException e) {
            throw new RuntimeException("General I/O Exception reading " + configFileRelativePath, e);
        }
        // apply the namespace prefix, if there is one
        kafkaConfiguration.getTopics().stream().forEach(topicConfig ->
                topicConfig.setTopic(namespacePrefix + topicConfig.getTopic()));

        return kafkaConfiguration;
    }

    /**
     * Apply the kafka topic configurations in KafkaConfiguration to a kafka cluster connected to by
     * the AdminClient.
     *
     * @param configuration the KafkaConfiguration to apply
     */
    public void applyKafkaConfiguration(@Nonnull KafkaConfiguration configuration)
            throws InterruptedException, ExecutionException {

        // create an admin client
        try (AdminClient adminClient = createKafkaAdminClient(kafkaBootstrapServers)) {
            // get the set of topics that need to be created
            ListTopicsResult topicListResult = adminClient.listTopics();
            // just block until we get the answer
            Set<String> topicNames = topicListResult.names().get();
            // now that we have a set of existing topics, find the ones we need to add.
            Set<NewTopic> newTopics = configuration.getTopics().stream()
                    .filter(topicConfig -> !topicNames.contains(topicConfig.getTopic())) // only topics that don't exist
                    .map(topicConfig -> new NewTopic(topicConfig.getTopic(),
                            topicConfig.getPartitions(),
                            topicConfig.getReplicationFactor()))
                    .collect(Collectors.toSet());

            long startTime = System.currentTimeMillis();
            // create the topics
            if (newTopics.size() > 0) {
                log.info("Will create new topics for: {}", newTopics);
                CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);

                // wait until all topics completed.
                createTopicsResult.all().get();
                long topicsCreatedTime = System.currentTimeMillis();
                log.info("Topics created successfully in {} ms.", topicsCreatedTime - startTime);
            }

            // now set any custom properties on topics
            // first create a set of config updates for each topic that needs them.
            Map<ConfigResource, Config> kafkaConfigs = configuration.getTopics().stream()
                    .filter(TopicConfiguration::hasProperties) // only include topics that have property overrides
                    .collect(Collectors.toMap(
                            topicConfig -> new ConfigResource(Type.TOPIC, topicConfig.getTopic()), // keys are topic ConfigResources
                            topicConfig -> new Config(topicConfig.getProperties().entrySet().stream() // values are property sets
                                    .map(entry -> new ConfigEntry(entry.getKey(), entry.getValue().toString()))
                                    .collect(Collectors.toSet()))
                            )
                    );
            log.info("Applying configurations {}", kafkaConfigs);
            // block until complete
            adminClient.alterConfigs(kafkaConfigs).all().get();
            log.info("Kafka configurations applied successfully in {} ms.",
                    System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Value class that is read from the kafka config yaml. Contains a list of topics w/override settings
     * for each
     */
    static public class KafkaConfiguration {
        private List<TopicConfiguration> topics;

        public List<TopicConfiguration> getTopics() {
            return topics;
        }

        public void setTopics(List<TopicConfiguration> topics) {
            this.topics = topics;
        }
    }

    /**
     * Value class read from the kafka config yaml. Represents one topic w/optional topic-specific
     * properties.
     */
    static public class TopicConfiguration {
        private String topic;
        private short replicationFactor = DEFAULT_TOPIC_REPLICATION_FACTOR;
        private short partitions = DEFAULT_TOPIC_PARTITION_COUNT;
        private Map<String, Object> properties;

        public void setTopic(String name) {
            this.topic = name;
        }
        public String getTopic() {
            return topic;
        }

        public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }
        public short getReplicationFactor() { return replicationFactor; }

        public void setPartitions(short partitions) { this.partitions = partitions; }
        public short getPartitions() { return partitions; }

        boolean hasProperties() {
            return this.properties != null;
        }
        public void setProperties(Map<String,Object> newProperties) {
            this.properties = newProperties;
        }
        public Map<String,Object> getProperties() {
            return properties;
        }
    }
}
