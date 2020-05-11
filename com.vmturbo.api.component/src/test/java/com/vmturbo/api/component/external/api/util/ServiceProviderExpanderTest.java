package com.vmturbo.api.component.external.api.util;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

public class ServiceProviderExpanderTest {

    private final SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);

    private final UuidMapper uuidMapper = mock(UuidMapper.class);

    private ServiceProviderExpander serviceProviderExpander;

    @Before
    public void setup() {
        serviceProviderExpander = new ServiceProviderExpander(uuidMapper, supplyChainFetcherFactory);
    }

    /**
     * Test expanding service provider works and also return the other scopes included.
     */
    @Test
    public void testExpandServiceProviders() {
        final ApiId scope1 = ApiTestUtils.mockEntityId("1", uuidMapper);
        final ApiId scope2 = ApiTestUtils.mockEntityId("2", uuidMapper);
        final ApiId scope3 = ApiTestUtils.mockEntityId("3", uuidMapper);

        when(uuidMapper.fromOid(1L)).thenReturn(scope1);
        when(uuidMapper.fromOid(2L)).thenReturn(scope2);
        when(uuidMapper.fromOid(1L).getClassName()).thenReturn(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        when(uuidMapper.fromOid(2L).getClassName()).thenReturn(ApiEntityType.DATABASE.apiStr());
        when(uuidMapper.fromOid(3L).getClassName()).thenReturn(ApiEntityType.SERVICE_PROVIDER.apiStr());

        Set<Long> expandedScope;
        when(supplyChainFetcherFactory.expandServiceProviders(anySetOf(Long.class))).thenReturn(Collections.emptySet());
        expandedScope = serviceProviderExpander.expand(ImmutableSet.of(scope1.oid(), scope2.oid()));
        assertTrue(expandedScope.contains(scope1.oid()));
        assertTrue(expandedScope.contains(scope2.oid()));

        when(supplyChainFetcherFactory.expandServiceProviders(ImmutableSet.of(3L))).thenReturn(ImmutableSet.of(4L));
        expandedScope = serviceProviderExpander.expand(ImmutableSet.of(scope1.oid(), scope2.oid(), scope3.oid()));
        assertTrue(expandedScope.contains(scope1.oid()));
        assertTrue(expandedScope.contains(scope2.oid()));
        assertTrue(expandedScope.contains(4L));
    }

}
