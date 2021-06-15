package com.vmturbo.api.component.external.api.util;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

public class ServiceProviderExpanderTest {

    private final SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);

    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private ServiceProviderExpander serviceProviderExpander;

    private final long realtimeContextId = 7;

    @Before
    public void setup() {
        serviceProviderExpander = new ServiceProviderExpander(repositoryApi, supplyChainFetcherFactory, realtimeContextId);
    }

    /**
     * Test expanding service provider works and also return the other scopes included.
     */
    @Test
    public void testExpandServiceProviders() {
        Set<Long> providerOid = Sets.newHashSet(3L);
        SearchRequest searchReq = ApiTestUtils.mockSearchIdReq(providerOid);
        when(repositoryApi.newSearchRequest(
                SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.entityTypeFilter(ApiEntityType.SERVICE_PROVIDER)).build()))
                .thenReturn(searchReq);

        Set<Long> expandedScope;
        when(supplyChainFetcherFactory.expandServiceProviders(anySetOf(Long.class))).thenReturn(Collections.emptySet());
        expandedScope = serviceProviderExpander.expand(ImmutableSet.of(1L, 2L));
        assertTrue(expandedScope.contains(1L));
        assertTrue(expandedScope.contains(2L));

        when(supplyChainFetcherFactory.expandServiceProviders(ImmutableSet.of(3L))).thenReturn(ImmutableSet.of(4L));
        expandedScope = serviceProviderExpander.expand(ImmutableSet.of(1L, 2L, 3L));
        assertTrue(expandedScope.contains(1L));
        assertTrue(expandedScope.contains(2L));
        assertTrue(expandedScope.contains(4L));

        // Only one repository RPC for service provider OIDS.
        verify(repositoryApi, times(1)).newSearchRequest(any());
    }

    /**
     * Test that the OIDs of service providers are cached and cleared when a new topology is available.
     */
    @Test
    public void testClearCachedOids() {
        Set<Long> providerOid = Sets.newHashSet(2L);
        SearchRequest searchReq = ApiTestUtils.mockSearchIdReq(providerOid);
        when(repositoryApi.newSearchRequest(
                SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.entityTypeFilter(ApiEntityType.SERVICE_PROVIDER)).build()))
                .thenReturn(searchReq);

        serviceProviderExpander.expand(Sets.newHashSet(1L));
        verify(repositoryApi, times(1)).newSearchRequest(any());

        serviceProviderExpander.expand(Sets.newHashSet(1L));
        // No additional RPC.
        verify(repositoryApi, times(1)).newSearchRequest(any());

        // Plan topology available
        serviceProviderExpander.onSourceTopologyAvailable(1, realtimeContextId + 1);
        serviceProviderExpander.expand(Sets.newHashSet(1L));
        // No additional RPC.
        verify(repositoryApi, times(1)).newSearchRequest(any());

        // Realtime topology available
        serviceProviderExpander.onSourceTopologyAvailable(1, realtimeContextId);
        serviceProviderExpander.expand(Sets.newHashSet(1L));
        // Fetch oids again.
        verify(repositoryApi, times(2)).newSearchRequest(any());
    }

}
