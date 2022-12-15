package com.vmturbo.cost.component.scope;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link OidMappingsBatch}.
 */
public class OidMappingsBatchTest {

    private OidMappingsBatch oidMappingsBatch;

    /**
     * Initialize test resources.
     */
    @Before
    public void setup() {
        oidMappingsBatch = new OidMappingsBatch(new HashMap<>());
    }

    /**
     * Test that if {@link OidMappingsBatch} is empty,
     * then {@link OidMappingsBatch#getAliasOids()} returns an empty Collection.
     */
    @Test
    public void testGetAliasOidsInSegmentEmpty() {
        Assert.assertTrue(oidMappingsBatch.getAliasOids().isEmpty());
    }

    /**
     * Test that {@link OidMappingsBatch#getAliasOids()} returns a collection containing all the alias oids
     * present in {@link OidMappingsBatch}.
     */
    @Test
    public void testGetAliasOidsInSegmentValidData() {
        final long aliasOid = 111111L;
        final Map<Long, Set<OidMapping>> newOidMappings = Collections.singletonMap(aliasOid,
            Collections.singleton(ImmutableOidMapping.builder()
                    .oidMappingKey(ImmutableOidMappingKey.builder()
                        .aliasOid(aliasOid).realOid(222222L)
                        .build())
                    .firstDiscoveredTimeMsUtc(Instant.now())
                .build()));
        oidMappingsBatch = new OidMappingsBatch(newOidMappings);
        Assert.assertEquals(Collections.singleton(aliasOid), oidMappingsBatch.getAliasOids());
    }
}