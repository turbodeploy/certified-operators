package com.vmturbo.mediation.udt.explore;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.ManagedChannel;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Test class for {@link Connection}.
 */
public class ConnectionTest {

    /**
     * Tests that gPRC channels are not changing.
     */
    @Test
    public void testConnectionFields() {
        ManagedChannel groupChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel repositoryChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel tpChannel = Mockito.mock(ManagedChannel.class);
        TopologyProcessor tpApi = Mockito.mock(TopologyProcessor.class);
        Connection connection = new Connection(groupChannel, repositoryChannel, tpChannel, tpApi);
        Assert.assertEquals(groupChannel, connection.getGroupChannel());
        Assert.assertEquals(repositoryChannel, connection.getRepositoryChannel());
    }

    /**
     * Tests that channels are close when 'release' is called.
     */
    @Test
    public void testConnectionRelease() {
        ManagedChannel groupChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel repositoryChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel tpChannel = Mockito.mock(ManagedChannel.class);
        TopologyProcessor tpApi = Mockito.mock(TopologyProcessor.class);
        Connection connection = new Connection(groupChannel, repositoryChannel, tpChannel, tpApi);
        connection.release();
        verify(groupChannel, times(1)).shutdownNow();
        verify(repositoryChannel, times(1)).shutdownNow();
    }
}
