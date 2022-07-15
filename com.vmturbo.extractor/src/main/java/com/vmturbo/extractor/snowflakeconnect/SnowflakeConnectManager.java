package com.vmturbo.extractor.snowflakeconnect;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.extractor.snowflakeconnect.client.SnowflakeConnectClient;
import com.vmturbo.extractor.snowflakeconnect.model.Config;
import com.vmturbo.extractor.snowflakeconnect.model.Connector;

/**
 * This object drives the snowflake connector's initialization process by making all necessary calls
 * to the kafka-connect REST API. It ensures that the connector will push to the Snowflake database
 * & schema defined in the configuration.
 */
public class SnowflakeConnectManager implements RequiresDataInitialization {

    private static final Logger logger = LogManager.getLogger();

    private final SnowflakeConnectClient snowflakeConnectClient;

    private final SnowflakeConnectManagerConfig config;

    private CompletableFuture<Connector> initializationResult = new CompletableFuture<>();

    SnowflakeConnectManager(SnowflakeConnectClient snowflakeConnectClient,
            SnowflakeConnectManagerConfig config) {
        this.snowflakeConnectClient = snowflakeConnectClient;
        this.config = config;
    }

    /**
     * Starts a thread that attempts to configure the snowflake connector with retries.
     */
    @Override
    public void initialize() {
        new Thread(() -> {
            boolean initialized = false;
            while (!initialized) {
                try {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    Connector connector = initializeSnowflakeConnector();
                    logger.info(
                            "The snowflake-connect initialization completed successfully in {}ms",
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
                    initialized = true;
                    initializationResult.complete(connector);
                } catch (RuntimeException e) {
                    logger.error("Failed to initialize snowflake-connect due to:", e);
                    try {
                        Thread.sleep(config.getRetryPeriodMs());
                    } catch (InterruptedException interruptedException) {
                        logger.error(
                                "The snowflake-connect initialization thread was interrupted. Exiting without completing initialization.",
                                interruptedException);
                        break;
                    }
                }
            }
        }, "snowflake-connect-initialization").start();
    }

    @VisibleForTesting
    CompletableFuture<Connector> getInitializationResult() {
        return initializationResult;
    }

    private Connector initializeSnowflakeConnector() {

        // check if the connector exists
        List<String> existingConnectors = snowflakeConnectClient.getConnectors();

        // if it does, compare the current config with the requested one
        if (existingConnectors.contains(config.getConnectorName())) {
            Connector current = snowflakeConnectClient.getConnector(config.getConnectorName());

            // if it is the same, we are done
            if (current.getConfig().equals(config.getConnectorConfig())) {
                logger.info(
                        "The snowflake connector is already configured with the requested values");
                return current;
            }

            // if it does not match the set values, delete the connector
            snowflakeConnectClient.deleteConnector(config.getConnectorName());
        }

        // we have either deleted the connector or it never existed, so we create the connector
        return snowflakeConnectClient.createConnector(config.getConnectorConfig());
    }

    /**
     * Object that holds all values needed by the {@link SnowflakeConnectManager}.
     */
    static class SnowflakeConnectManagerConfig {

        private long retryPeriodMs;
        private String connectorName;
        private Config connectorConfig;

        long getRetryPeriodMs() {
            return retryPeriodMs;
        }

        SnowflakeConnectManagerConfig setRetryPeriodMs(long retryPeriodMs) {
            this.retryPeriodMs = retryPeriodMs;
            return this;
        }

        String getConnectorName() {
            return connectorName;
        }

        SnowflakeConnectManagerConfig setConnectorName(String connectorName) {
            this.connectorName = connectorName;
            return this;
        }

        Config getConnectorConfig() {
            return connectorConfig;
        }

        SnowflakeConnectManagerConfig setConnectorConfig(Config connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }
    }
}
