package com.vmturbo.api.component.external.api.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

public class StatsUtilsTest {

    /**
     * Test if the scope is valid for the RI query.
     */
    @Test
    public void testIsValidScopeForRIBoughtQuery() {
        // Business Account
        ApiId apiId = mockApiId(ImmutableSet.of(ApiEntityType.BUSINESS_ACCOUNT), false);
        boolean isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Zone
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.AVAILABILITY_ZONE), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Region
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.REGION), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Provider
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.SERVICE_PROVIDER), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Plan
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE), true);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertTrue(isValid);

        // Not Valid
        apiId = mockApiId(ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE), false);
        isValid = StatsUtils.isValidScopeForRIBoughtQuery(apiId);
        assertFalse(isValid);
    }

    @NotNull
    private ApiId mockApiId(@NotNull final Set<ApiEntityType> entityTypes, boolean isCloudPlan) {
        Optional<Set<ApiEntityType>> types = Optional.of(entityTypes);
        ApiId apiId = Mockito.mock(ApiId.class);
        Mockito.when(apiId.isCloudPlan()).thenReturn(isCloudPlan);
        Mockito.when(apiId.getScopeTypes()).thenReturn(types);

        return apiId;
    }

}
