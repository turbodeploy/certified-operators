package com.vmturbo.extractor.snowflakeconnect;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;

import com.vmturbo.extractor.snowflakeconnect.SnowflakeConnectManager.SnowflakeConnectManagerConfig;
import com.vmturbo.extractor.snowflakeconnect.client.SnowflakeConnectClient;
import com.vmturbo.extractor.snowflakeconnect.model.Config;
import com.vmturbo.extractor.snowflakeconnect.model.Connector;

/**
 * Tests the snowflake-connect configuration.
 */
public class SnowflakeConnectManagerTest {

    @Mock
    private SnowflakeConnectClient snowflakeConnectClient;

    private static final Connector connector11 = createConnector("connector-1", "topic-1");
    private static final Connector connector12 = createConnector("connector-1", "topic-2");

    /**
     * Init REST API client mock.
     */
    @Before
    public void setUp() {
        snowflakeConnectClient = mock(SnowflakeConnectClient.class);
    }

    /**
     * Tests that if no connector is configured, it will be created.
     *
     * @throws ExecutionException because we wait for the initialization to finish
     * @throws InterruptedException because we wait for the initialization to finish
     * @throws TimeoutException because we wait for the initialization to finish
     */
    @Test
    public void testNewConnector()
            throws ExecutionException, InterruptedException, TimeoutException {

        // ARRANGE
        when(snowflakeConnectClient.getConnectors()).thenReturn(Collections.emptyList());
        when(snowflakeConnectClient.createConnector(any(Config.class))).thenReturn(connector11);

        SnowflakeConnectManagerConfig managerConfig = getManagerConfig(connector11);
        SnowflakeConnectManager snowflakeConnectManager =
                new SnowflakeConnectManager(snowflakeConnectClient, managerConfig);

        // ACT
        snowflakeConnectManager.initialize();

        // ASSERT
        Connector connector =
                snowflakeConnectManager.getInitializationResult().get(1, TimeUnit.MINUTES);
        Assert.assertEquals(connector11.getConfig(), connector.getConfig());
    }

    /**
     * Tests that if a <b>different</b> connector is already configured, it will be deleted and the
     * correct connector will be created.
     *
     * @throws ExecutionException because we wait for the initialization to finish
     * @throws InterruptedException because we wait for the initialization to finish
     * @throws TimeoutException because we wait for the initialization to finish
     */
    @Test
    public void testAlreadyExistingButDifferent()
            throws ExecutionException, InterruptedException, TimeoutException {
        // ARRANGE - connector11 is configured
        when(snowflakeConnectClient.getConnectors()).thenReturn(
                Collections.singletonList(connector11.getName()));
        when(snowflakeConnectClient.getConnector(connector11.getName())).thenReturn(connector11);
        when(snowflakeConnectClient.createConnector(any(Config.class))).thenReturn(connector12);

        // the manager sees that we want to have connector12 (instead of topic-1, topic-2)
        SnowflakeConnectManagerConfig managerConfig = getManagerConfig(connector12);
        SnowflakeConnectManager snowflakeConnectManager =
                new SnowflakeConnectManager(snowflakeConnectClient, managerConfig);

        // ACT
        snowflakeConnectManager.initialize();

        // ASSERT
        Connector connector =
                snowflakeConnectManager.getInitializationResult().get(1, TimeUnit.MINUTES);
        Assert.assertEquals(connector12.getConfig(), connector.getConfig());
        verify(snowflakeConnectClient, times(1)).deleteConnector(connector11.getName());
    }

    /**
     * Tests that if the <b>same</b> connector is already configured, the initialization completes
     * without making any changes.
     *
     * @throws ExecutionException because we wait for the initialization to finish
     * @throws InterruptedException because we wait for the initialization to finish
     * @throws TimeoutException because we wait for the initialization to finish
     */
    @Test
    public void testAlreadyExisting()
            throws ExecutionException, InterruptedException, TimeoutException {
        // ARRANGE
        when(snowflakeConnectClient.getConnectors()).thenReturn(
                Collections.singletonList(connector11.getName()));
        when(snowflakeConnectClient.getConnector(connector11.getName())).thenReturn(connector11);

        SnowflakeConnectManagerConfig managerConfig = getManagerConfig(connector11);
        SnowflakeConnectManager snowflakeConnectManager =
                new SnowflakeConnectManager(snowflakeConnectClient, managerConfig);

        // ACT
        snowflakeConnectManager.initialize();

        // ASSERT
        Connector connector =
                snowflakeConnectManager.getInitializationResult().get(1, TimeUnit.MINUTES);
        Assert.assertEquals(connector11.getConfig(), connector.getConfig());
        verify(snowflakeConnectClient, times(0)).deleteConnector(anyString());
        verify(snowflakeConnectClient, times(0)).createConnector(any(Config.class));
    }

    /**
     * Tests that even if network/server errors occur the retry mechanism will work and eventually
     * the initialization will complete.
     *
     * @throws ExecutionException because we wait for the initialization to finish
     * @throws InterruptedException because we wait for the initialization to finish
     * @throws TimeoutException because we wait for the initialization to finish
     */
    @Test
    public void testRetriesWork() throws ExecutionException, InterruptedException, TimeoutException {
        // ARRANGE
        when(snowflakeConnectClient.getConnectors())
                .thenThrow(new HttpServerErrorException(HttpStatus.GATEWAY_TIMEOUT))
                .thenReturn(Collections.singletonList(connector11.getName()));
        when(snowflakeConnectClient.getConnector(connector11.getName()))
                .thenThrow(new HttpServerErrorException(HttpStatus.GATEWAY_TIMEOUT))
                .thenReturn(connector11);
        when(snowflakeConnectClient.createConnector(any(Config.class)))
                .thenThrow(new HttpServerErrorException(HttpStatus.GATEWAY_TIMEOUT))
                .thenReturn(connector12);

        SnowflakeConnectManagerConfig managerConfig = getManagerConfig(connector12);
        SnowflakeConnectManager snowflakeConnectManager =
                new SnowflakeConnectManager(snowflakeConnectClient, managerConfig);

        // ACT
        snowflakeConnectManager.initialize();

        // ASSERT
        Connector connector =
                snowflakeConnectManager.getInitializationResult().get(1, TimeUnit.MINUTES);
        Assert.assertEquals(connector12.getConfig(), connector.getConfig());
        verify(snowflakeConnectClient, times(4)).getConnectors();
        verify(snowflakeConnectClient, times(3)).getConnector(connector11.getName());
        verify(snowflakeConnectClient, times(2)).deleteConnector(connector11.getName());
        verify(snowflakeConnectClient, times(2)).createConnector(connector12.getConfig());
    }

    @Nonnull
    private static SnowflakeConnectManagerConfig getManagerConfig(Connector connector) {
        return new SnowflakeConnectManagerConfig()
                .setConnectorConfig(connector.getConfig())
                .setConnectorName(connector.getName())
                .setRetryPeriodMs(1);
    }

    @Nonnull
    private static Connector createConnector(String name, String topic) {
        Config connectorConfig = getConfig(name, topic);
        Connector connector = new Connector();
        connector.setConfig(connectorConfig);
        connector.setName(name);
        return connector;
    }

    @Nonnull
    private static Config getConfig(String name, String topic) {
        Config connectorConfig = new Config();
        connectorConfig.setName(name);
        connectorConfig.setConnectorClass("");
        connectorConfig.setValueConverter("");
        connectorConfig.setKeyConverter("");
        connectorConfig.setTopics(topic);
        connectorConfig.setSnowflakeTopic2TableMap("");
        connectorConfig.setSnowflakeUrlName("");
        connectorConfig.setSnowflakeDatabaseName("");
        connectorConfig.setSnowflakeSchemaName("");
        connectorConfig.setSnowflakeUserName("");
        connectorConfig.setSnowflakePrivateKey("");
        return connectorConfig;
    }
}