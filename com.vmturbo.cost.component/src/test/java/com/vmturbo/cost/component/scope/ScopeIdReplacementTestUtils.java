package com.vmturbo.cost.component.scope;

import java.time.Instant;

/**
 * Test data and other utility for Scope id replacement related tests.
 */
public class ScopeIdReplacementTestUtils {

    private ScopeIdReplacementTestUtils() {
    }

    /**
     * test data alias oid.
     */
    public  static final long ALIAS_OID = 111111L;
    /**
     * test data real oid.
     */
    public  static final long REAL_OID = 7777777L;
    /**
     * test data real oid.
     */
    public  static final long REAL_OID_2 = 8888888L;
    /**
     * test data real oid.
     */
    public  static final long REAL_OID_3 = 9999999L;
    /**
     * test first discovered time.
     */
    public  static final long BROADCAST_TIME_UTC_MS = 1668046796000L;
    /**
     * test first discovered time.
     */
    public  static final long BROADCAST_TIME_UTC_MS_NEXT_DAY = 1668133196000L;


    /**
     * Creates an instance of {@link OidMapping} with the given inputs.
     *
     * @param aliasOid for the {@link OidMapping}.
     * @param realOid for the {@link OidMapping}.
     * @param firstDiscoveredTime for the {@link OidMapping}.
     * @return an instance of {@link OidMapping}.
     */
    public static OidMapping createOidMapping(final long aliasOid, final long realOid, final long firstDiscoveredTime) {
        return ImmutableOidMapping.builder()
            .oidMappingKey(ImmutableOidMappingKey.builder()
                .aliasOid(aliasOid)
                .realOid(realOid)
                .build())
            .firstDiscoveredTimeMsUtc(Instant.ofEpochMilli(firstDiscoveredTime))
            .build();
    }
}