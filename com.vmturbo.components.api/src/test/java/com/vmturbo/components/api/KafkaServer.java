package com.vmturbo.components.api;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.rules.ExternalResource;

import com.google.common.io.Files;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

/**
 * Rule to automatically start up Kafka server for testing purposes.
 */
public class KafkaServer extends ExternalResource {

    /**
     * Logger to use.
     */
    private final Logger logger = LogManager.getLogger(getClass());
    /**
     * Kafka instance reference.
     */
    private KafkaServerStartable kafka;
    /**
     * Thread pool for internal tasks.
     */
    private ExecutorService threadPool;
    /**
     * Temporary directory to locate all the files. Will be removed automatically after tests
     * finishes.
     */
    private File tmpDir;
    /**
     * Bootstrap servers configuration.
     */
    private String bootstrapServers;

    @Override
    protected void before() throws IOException {
        threadPool = Executors.newCachedThreadPool();
        tmpDir = Files.createTempDir();
        kafka = configureKafka();
        startKafka();
    }

    @Override
    protected void after() {
        stopKafka();
        threadPool.shutdownNow();
        try {
            FileUtils.forceDelete(tmpDir);
        } catch (IOException e) {
            logger.warn("Could not remove temporary directory " + tmpDir, e);
        }
    }

    /**
     * Creates a directory in a temporary storage. The new directory will be removed after the
     * test run.
     *
     * @param dirName a name for new directory
     * @return reference for a newly created directory
     * @throws IOException if IO exception occurs.
     */
    @Nonnull
    private File createTempDir(@Nonnull String dirName) throws IOException {
        final File newDir = new File(tmpDir, dirName);
        FileUtils.forceMkdir(newDir);
        return newDir;
    }

    @Nonnull
    private KafkaServerStartable configureKafka() throws IOException {
        logger.info("starting local zookeeper...");
        final int zooPort = startZookeeper();
        logger.info("done");

        final Properties kafkaProperties = new Properties();
        kafkaProperties.put("zookeeper.connect", "localhost:" + zooPort);
        final int kafkaPort = getFreePort();
        kafkaProperties.put("listeners", "PLAINTEXT://:" + kafkaPort);
        kafkaProperties.put("offsets.topic.replication.factor", Integer.toString(1));
        kafkaProperties.put("log.dir", createTempDir("kafka-logs-dir").toString());
        // Restrict messages size to 10 MB for tests
        kafkaProperties.put("message.max.bytes", Integer.toString(1024 * 1024 * 10));
        this.bootstrapServers = "localhost:" + kafkaPort;
        final KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        final KafkaServerStartable kafka = new KafkaServerStartable(kafkaConfig);
        return kafka;
    }

    public void startKafka() throws IOException {
        logger.info("starting local kafka broker...");
        kafka.startup();
        logger.info("done");
    }

    public void stopKafka() {
        kafka.shutdown();
        kafka.awaitShutdown();
    }

    private int startZookeeper() throws IOException {
        final Properties zkProperties = new Properties();

        zkProperties.setProperty("dataDir", createTempDir("zookeeper-data-dir").toString());
        final int zooPort = getFreePort();
        zkProperties.setProperty("clientPort", Integer.toString(zooPort));
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);
        threadPool.submit(() -> {
            try {

                zooKeeperServer.runFromConfig(configuration);
            } catch (IOException | AdminServerException e) {
                System.out.println("ZooKeeper Failed");
                e.printStackTrace(System.err);
            }
        });
        return zooPort;
    }

    /**
     * Method searches for a free TCP port to use. After TCP port has been closed, it will hang
     * in the closed state for some time (30 sec). It is safe to grab the port within this interval.
     *
     * @return port number available to reuse
     * @throws IOException if IO exception occurs.
     */
    private int getFreePort() throws IOException {
        try (final ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Returns bootstrap servers configuration for the Kafka instance started.
     *
     * @return bootstrap servers
     */
    @Nonnull
    public String getBootstrapServers() {
        if (bootstrapServers != null) {
            return this.bootstrapServers;
        } else {
            throw new IllegalStateException("Kafka instance has not been yet started. Bootstrap " +
                    "server information is unavailable");
        }
    }
}