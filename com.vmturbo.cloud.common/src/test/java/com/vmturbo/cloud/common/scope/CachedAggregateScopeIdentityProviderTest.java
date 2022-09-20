package com.vmturbo.cloud.common.scope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity.CloudScopeType;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;

/**
 * Test class for {@link CachedAggregateScopeIdentityProvider}.
 */
public class CachedAggregateScopeIdentityProviderTest {

    private static final CloudScope AGGREGATE_CLOUD_SCOPE = CloudScope.builder()
            .accountId(111L)
            .regionId(222L)
            .serviceProviderId(333L)
            .build();

    @Mock
    private CloudScopeIdentityStore scopeIdentityStore;

    @Mock
    private IdentityProvider identityProvider;

    private CachedAggregateScopeIdentityProvider scopeIdentityProvider;

    /**
     * Setup.
     */
    @Before
    public void setup() {

        MockitoAnnotations.initMocks(this);

        scopeIdentityProvider = new CachedAggregateScopeIdentityProvider(scopeIdentityStore, identityProvider, Duration.ofMillis(10));

        when(scopeIdentityStore.getIdentitiesByFilter(any())).thenReturn(Collections.emptyList());
    }

    /**
     * Test that an aggregate scope identity loaded on cache initialization is correctly returned
     * on subsequent requests for a cloud scope identity of the same cloud scope.
     */
    @Test
    public void testCacheConsistency() throws IdentityOperationException, IdentityUninitializedException, InitializationException {

        final CloudScopeIdentity firstScopeIdentity = CloudScopeIdentity.builder()
                .populateFromCloudScope(AGGREGATE_CLOUD_SCOPE)
                .scopeId(123L)
                .scopeType(CloudScopeType.AGGREGATE)
                .build();
        final CloudScopeIdentity secondScopeIdentity = CloudScopeIdentity.builder()
                .populateFromCloudScope(AGGREGATE_CLOUD_SCOPE)
                .scopeId(456L)
                .scopeType(CloudScopeType.AGGREGATE)
                .build();

        // set up the persistence store
        when(scopeIdentityStore.getIdentitiesByFilter(any())).thenReturn(ImmutableList.of(firstScopeIdentity, secondScopeIdentity));

        // initialize the store
        scopeIdentityProvider.initialize();

        // Verify the first scope identity is correctly returned
        final Map<CloudScope, CloudScopeIdentity> actualScopeIdentityMap =
                scopeIdentityProvider.getOrCreateScopeIdentities(ImmutableList.of(AGGREGATE_CLOUD_SCOPE));

        assertThat(actualScopeIdentityMap, hasKey(AGGREGATE_CLOUD_SCOPE));
        assertThat(actualScopeIdentityMap.get(AGGREGATE_CLOUD_SCOPE), equalTo(firstScopeIdentity));
    }

    /**
     * Verify the same scope identity is assigned to an aggregate scope on multiple calls.
     */
    @Test
    public void testAggregateAssignmentConsistency() throws IdentityOperationException, IdentityUninitializedException, InitializationException {

        // initialize the store
        scopeIdentityProvider.initialize();

        final Map<CloudScope, CloudScopeIdentity> firstScopeIdentities =
                scopeIdentityProvider.getOrCreateScopeIdentities(ImmutableList.of(AGGREGATE_CLOUD_SCOPE));
        final Map<CloudScope, CloudScopeIdentity> secondScopeIdentities =
                scopeIdentityProvider.getOrCreateScopeIdentities(ImmutableList.of(AGGREGATE_CLOUD_SCOPE));

        // Verify the scope identities match
        assertThat(firstScopeIdentities.entrySet(), hasSize(1));
        assertThat(secondScopeIdentities.entrySet(), hasSize(1));
        assertThat(firstScopeIdentities, equalTo(secondScopeIdentities));
    }
}
