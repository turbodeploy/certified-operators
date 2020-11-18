package com.vmturbo.mediation.udt;

import static com.vmturbo.mediation.udt.TestUtils.getIProbeContext;
import static com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Set;

import io.grpc.ManagedChannel;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.mediation.udt.config.ConnectionConfiguration;
import com.vmturbo.mediation.udt.config.ConnectionProperties;
import com.vmturbo.mediation.udt.explore.Connection;
import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Test class for {@link UdtProbe}.
 */
public class UdtProbeTest {

    /**
     * Verify account class.
     */
    @Test
    public void testGetAccountDefinitionClass() {
        UdtProbe probe = new UdtProbe();
        assertEquals(UdtProbeAccount.class, probe.getAccountDefinitionClass());
    }

    /**
     * The method test that probe`s connection is released on probe destroying.
     */
    @Test
    public void testReleaseConnection() {
        Connection connection = Mockito.mock(Connection.class);
        UdtProbe probe = new UdtProbe();
        probe.initializeConnection(connection);
        probe.destroy();
        verify(connection, times(1)).release();
    }

    /**
     * Verify that there is an error if the probe has no connection.
     */
    @Test
    public void testOnNoConnection() {
        UdtProbe probe = new UdtProbe();
        DiscoveryResponse response = probe.discoverTarget(new UdtProbeAccount());
        Assert.assertEquals(1, response.getErrorDTOCount());
    }

    /**
     * Verify that there is no an error on verification.
     */
    @Test
    public void testValidate() {
        UdtProbe probe = new UdtProbe();
        Connection connection = Mockito.mock(Connection.class);
        probe.initializeConnection(connection);
        ValidationResponse response = probe.validateTarget(new UdtProbeAccount());
        Assert.assertEquals(0, response.getErrorDTOCount());
    }

    /**
     * Verify that there is an error if the connection is NULL.
     */
    @Test
    public void testValidateNoConnection() {
        UdtProbe probe = new UdtProbe();
        ValidationResponse response = probe.validateTarget(new UdtProbeAccount());
        Assert.assertEquals(1, response.getErrorDTOCount());
    }

    /**
     * Tests that the supply chain is defined.
     */
    @Test
    public void testSupplyChain() {
        UdtProbe probe = new UdtProbe();
        Set<TemplateDTO> templates = probe.getSupplyChainDefinition();
        Assert.assertNotNull(templates);
        Assert.assertFalse(templates.isEmpty());
    }

    /**
     * Tests that probe`s initialization does not produce exceptions.
     */
    @Test
    @Ignore
    public void testInitialize() {
        System.setProperty("propertiesYamlPath", "properties.yaml");
        UdtProbe probe = new UdtProbe();
        probe.initialize(getIProbeContext(), null);
    }

    /**
     * Tests connection creating.
     */
    @Test
    public void testCreateConnection() {
        ConnectionProperties props = new ConnectionProperties("group", "repository", "tp", 9001, 300);
        ConnectionConfiguration configuration = new ConnectionConfiguration(props, Mockito.mock(TopologyProcessorClientConfig.class));
        UdtProbe probe = new UdtProbe();
        Connection connection = probe.createProbeConnection(configuration);
        Assert.assertNotNull(connection);
        probe.destroy();
    }

    /**
     * Tests DataProvider creating.
     */
    @Test
    public void testBuildDataProvider() {
        UdtProbe probe = new UdtProbe();
        Connection connection = Mockito.mock(Connection.class);
        ManagedChannel groupChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel repoChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel tpChannel = Mockito.mock(ManagedChannel.class);
        Mockito.when(connection.getGroupChannel()).thenReturn(groupChannel);
        Mockito.when(connection.getRepositoryChannel()).thenReturn(repoChannel);
        Mockito.when(connection.getTopologyProcessorChannel()).thenReturn(tpChannel);
        DataProvider dataProvider = probe.buildDataProvider(connection);
        Assert.assertNotNull(dataProvider);
    }

}
