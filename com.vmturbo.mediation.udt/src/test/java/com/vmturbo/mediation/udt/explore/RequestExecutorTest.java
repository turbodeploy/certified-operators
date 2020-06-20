package com.vmturbo.mediation.udt.explore;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.ManagedChannel;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link RequestExecutor}.
 */
public class RequestExecutorTest {

    /**
     * The method tests that 'RequestExecutor' correctly calls Connection`s channels.
     */
    @Test
    public void testCreateServices() {
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.getGroupChannel()).thenReturn(Mockito.mock(ManagedChannel.class));
        Mockito.when(connection.getRepositoryChannel()).thenReturn(Mockito.mock(ManagedChannel.class));
        new RequestExecutor(connection);
        verify(connection, times(2)).getGroupChannel();
        verify(connection, times(2)).getRepositoryChannel();
    }

    /**
     * The method tests that services uses correct gRPC channels.
     */
    @Test
    public void testChannelsUsage() {
        Connection connection = Mockito.mock(Connection.class);
        ManagedChannel groupChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel repoChannel = Mockito.mock(ManagedChannel.class);
        Mockito.when(connection.getGroupChannel()).thenReturn(groupChannel);
        Mockito.when(connection.getRepositoryChannel()).thenReturn(repoChannel);
        RequestExecutor requestExecutor = new RequestExecutor(connection);

        Assert.assertEquals(groupChannel, requestExecutor.getGroupService().getChannel());
        Assert.assertEquals(groupChannel, requestExecutor.getTopologyDataDefService().getChannel());
        Assert.assertEquals(repoChannel, requestExecutor.getSearchService().getChannel());
        Assert.assertEquals(repoChannel, requestExecutor.getRepositoryService().getChannel());
    }
}
