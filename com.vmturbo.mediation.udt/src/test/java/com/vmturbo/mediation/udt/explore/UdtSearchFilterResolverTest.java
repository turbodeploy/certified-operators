package com.vmturbo.mediation.udt.explore;

import java.util.Collection;
import java.util.Collections;

import io.grpc.ManagedChannel;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Unit tests for {@link UdtSearchFilterResolver}.
 */
public class UdtSearchFilterResolverTest {
    private final Connection connection = Mockito.mock(Connection.class);
    private final RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
    private final DataRequests dataRequests = Mockito.mock(DataRequests.class);
    private UdtSearchFilterResolver resolver;

    /**
     * Set up test.
     */
    @Before
    public void setUp() {
        ManagedChannel groupChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel repoChannel = Mockito.mock(ManagedChannel.class);
        ManagedChannel tpChannel = Mockito.mock(ManagedChannel.class);
        Mockito.when(connection.getGroupChannel()).thenReturn(groupChannel);
        Mockito.when(connection.getRepositoryChannel()).thenReturn(repoChannel);
        Mockito.when(connection.getTopologyProcessorChannel()).thenReturn(tpChannel);

        resolver = new UdtSearchFilterResolver(connection, requestExecutor, dataRequests);
    }

    /**
     * Test {@link UdtSearchFilterResolver#getGroupOwners(Collection, GroupType)} that
     * correctly calls {@link DataRequests} and {@link RequestExecutor}.
     */
    @Test
    public void testGetGroupOwner() {
        resolver.getGroupOwners(Collections.singletonList(1L), GroupType.RESOURCE);

        Mockito.verify(dataRequests).getGroupOwnerRequest(Mockito.anyCollection(),
                Mockito.eq(GroupType.RESOURCE));
        Mockito.verify(requestExecutor).getOwnersOfGroups(Mockito.any());
    }
}
