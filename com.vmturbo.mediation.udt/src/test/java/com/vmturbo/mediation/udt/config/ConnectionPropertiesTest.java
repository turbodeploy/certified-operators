package com.vmturbo.mediation.udt.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link  ConnectionProperties}.
 */
public class ConnectionPropertiesTest {

    /**
     * Verify ConnectionProperties`s fields values.
     */
    @Test
    public void testPropsValues() {
        String groupHost = "group";
        String repositoryHost = "repository";
        String tpHost = "topology-processor";
        int gRpcPort = 9001;
        int gRpcPingIntervalSeconds = 30;
        ConnectionProperties properties = new ConnectionProperties(groupHost, repositoryHost, tpHost, gRpcPort, gRpcPingIntervalSeconds);

        Assert.assertEquals(groupHost, properties.getGroupHost());
        Assert.assertEquals(repositoryHost, properties.getRepositoryHost());
        Assert.assertEquals(gRpcPort, properties.getgRpcPort());
        Assert.assertEquals(gRpcPingIntervalSeconds, properties.getgRpcPingIntervalSeconds());
    }
}
