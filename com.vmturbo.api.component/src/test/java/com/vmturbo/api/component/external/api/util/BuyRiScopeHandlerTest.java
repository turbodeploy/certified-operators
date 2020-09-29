package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Unit tests for {@link BuyRiScopeHandler} class.
 */
public class BuyRiScopeHandlerTest {

    private static final long OID_ACCOUNT_1 = 1L;
    private static final long OID_ACCOUNT_2 = 2L;
    private static final long OID_REGION_1 = 12L;
    private static final long OID_REGION_2 = 22L;
    private static final long OID_SERVICE_PROVIDER_1 = 10L;
    private static final long OID_SERVICE_PROVIDER_2 = 20L;

    private final BuyRiScopeHandler buyRiScopeHandler = new BuyRiScopeHandler();

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for global scope.
     */
    @Test
    public void testExtractActionTypesForGlobalScope() {
        testExtractActionTypesIncludingBuyRi(apiIdForGlobalScope());
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Billing Family scope.
     */
    @Test
    public void testExtractActionTypesForBillingFamily() {
        testExtractActionTypesIncludingBuyRi(apiIdForBillingFamily());
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for group of Billing Family scope.
     */
    @Test
    public void testExtractActionTypesForGroupOfBillingFamily() {
        testExtractActionTypesIncludingBuyRi(apiIdForGroupOfBillingFamily());
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Region scope.
     */
    @Test
    public void testExtractActionTypesForRegion() {
        testExtractActionTypesIncludingBuyRi(apiIdForRegion());
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Region group scope.
     */
    @Test
    public void testExtractActionTypesForRegionGroup() {
        testExtractActionTypesIncludingBuyRi(apiIdForRegionGroup());
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Service provider scope.
     */
    @Test
    public void testExtractActionTypesForServiceProviders() {
        testExtractActionTypesIncludingBuyRi(apiIdForServiceProviderScope());
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Service Provider group scope.
     */
    @Test
    public void testExtractActionTypesForServiceProvidersGroup() {
        testExtractActionTypesIncludingBuyRi(apiIdForServiceProviderScopeGroup());
    }

    private void testExtractActionTypesIncludingBuyRi(@Nonnull final ApiId inputScope) {
        // Arrange
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setActionTypeList(ImmutableList.of(ActionType.MOVE, ActionType.BUY_RI));

        // Act
        final Set<ActionDTO.ActionType> result = buyRiScopeHandler.extractActionTypes(
                inputDto, inputScope);

        // Assert
        // Buy RI type must be preserved
        assertThat(result,
                is(ImmutableSet.of(ActionDTO.ActionType.MOVE, ActionDTO.ActionType.BUY_RI)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Account scope.
     */
    @Test
    public void testExtractActionTypesForAccount() {
        // Arrange
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setActionTypeList(ImmutableList.of(ActionType.MOVE, ActionType.BUY_RI));
        final ApiId inputScope = apiIdForAccount();

        // Act
        final Set<ActionDTO.ActionType> result = buyRiScopeHandler.extractActionTypes(
                inputDto, inputScope);

        // Assert
        // Buy RI type must be excluded
        assertThat(result, is(ImmutableSet.of(ActionDTO.ActionType.MOVE)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Account scope when no action
     * types are provided in the input.
     */
    @Test
    public void testExtractActionTypesForAccountNoInput() {
        // Arrange
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        final ApiId inputScope = apiIdForAccount();

        // Act
        final Set<ActionDTO.ActionType> result = buyRiScopeHandler.extractActionTypes(
                inputDto, inputScope);

        // Assert
        // All action types must be returned except of Buy RI
        final Set<ActionDTO.ActionType> expectedResult = Arrays
                .stream(ActionDTO.ActionType.values())
                .filter(type -> type != ActionDTO.ActionType.BUY_RI)
                .collect(Collectors.toSet());
        assertThat(result, is(expectedResult));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method for Account scope when input
     * contains a single Buy RI action type.
     */
    @Test
    public void testExtractActionTypesForAccountWithSingleBuyRiInput() {
        // Arrange
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setActionTypeList(ImmutableList.of(ActionType.BUY_RI));
        final ApiId inputScope = apiIdForAccount();

        // Act
        final Set<ActionDTO.ActionType> result = buyRiScopeHandler.extractActionTypes(
                inputDto, inputScope);

        // Assert
        // NONE type must be returned to force empty response
        assertEquals(ImmutableSet.of(ActionDTO.ActionType.NONE), result);
    }

    /**
     * Test {@link BuyRiScopeHandler#extractActionTypes} method with no scope information.
     */
    @Test
    public void testExtractActionTypesForNoScope() {
        // Arrange
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setActionTypeList(ImmutableList.of(ActionType.MOVE, ActionType.BUY_RI));

        // Act
        final Set<ActionDTO.ActionType> result = buyRiScopeHandler.extractActionTypes(
                inputDto, null);

        // Assert
        assertEquals(ImmutableSet.of(ActionDTO.ActionType.MOVE), result);
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for global scope.
     */
    @Test
    public void testExtractBuyRiEntitiesForGlobalScope() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiIdForGlobalScope());

        // Assert
        assertThat(result, is(Collections.emptySet()));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for Billing Family.
     */
    @Test
    public void testExtractBuyRiEntitiesForBillingFamily() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiIdForBillingFamily());

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_ACCOUNT_1, OID_ACCOUNT_2)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for a group of
     * Billing Family.
     */
    @Test
    public void testExtractBuyRiEntitiesForGroupOfBillingFamily() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(
            apiIdForGroupOfBillingFamily());

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_ACCOUNT_1, OID_ACCOUNT_2)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for Region.
     */
    @Test
    public void testExtractBuyRiEntitiesForRegion() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiIdForRegion());

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_REGION_1)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for Region group.
     */
    @Test
    public void testExtractBuyRiEntitiesForRegionGroup() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiIdForRegionGroup());

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_REGION_1, OID_REGION_2)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for Service Provider.
     */
    @Test
    public void testExtractBuyRiEntitiesForServiceProvider() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiIdForServiceProviderScope());

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_SERVICE_PROVIDER_1)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for Service Provider group.
     */
    @Test
    public void testExtractBuyRiEntitiesForServiceProviderGroup() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiIdForServiceProviderScopeGroup());

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_SERVICE_PROVIDER_1, OID_SERVICE_PROVIDER_2)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method for Account.
     */
    @Test
    public void testExtractBuyRiEntitiesForAccount() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiIdForAccount());

        // Assert
        assertThat(result, is(Collections.emptySet()));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId)} method without scope scope.
     */
    @Test
    public void testExtractBuyRiEntitiesForNull() {
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(null);

        // Assert
        assertThat(result, is(Collections.emptySet()));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId, Set)} method without scope scope.
     */
    @Test
    public void testExtractBuyRiEntitiesForNullApiIdAndEmptyEntityTypeList() {
        // Given
        final ApiId apiId = null;
        final Set<Integer> entityTypeIds = Collections.emptySet();
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiId, entityTypeIds);

        // Assert
        assertThat(result, is(Collections.emptySet()));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId, Set)} method with
     * Billing Family scope and empty entity types.
     */
    @Test
    public void testExtractBuyRiEntitiesForBillingFamilyAndEmptyEntityTypeList() {
        // Given
        final ApiId apiId = apiIdForBillingFamily();
        final Set<Integer> entityTypeIds = Collections.emptySet();
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiId, entityTypeIds);

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_ACCOUNT_1, OID_ACCOUNT_2)));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId, Set)} method with
     * Billing Family scope and VIRTUAL_VOLUME entity type.
     */
    @Test
    public void testExtractBuyRiEntitiesForBillingFamilyAndOnlyVirtualVolumeType() {
        // Given
        final ApiId apiId = apiIdForBillingFamily();
        final Set<Integer> entityTypeIds = Collections.singleton(EntityType.VIRTUAL_VOLUME_VALUE);
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiId, entityTypeIds);

        // Assert
        assertThat(result, is(Collections.emptySet()));
    }

    /**
     * Test {@link BuyRiScopeHandler#extractBuyRiEntities(ApiId, Set)} method with
     * Billing Family scope and VM & VV entity types.
     */
    @Test
    public void testExtractBuyRiEntitiesForBillingFamilyAndVirtualVolumeTypeAndVirtualMachineType() {
        // Given
        final ApiId apiId = apiIdForBillingFamily();
        final Set<Integer> entityTypeIds = Sets.newHashSet(EntityType.VIRTUAL_VOLUME_VALUE, EntityType.VIRTUAL_MACHINE_VALUE);
        // Act
        final Set<Long> result = buyRiScopeHandler.extractBuyRiEntities(apiId, entityTypeIds);

        // Assert
        assertThat(result, is(ImmutableSet.of(OID_ACCOUNT_1, OID_ACCOUNT_2)));
    }

    private static ApiId apiIdForGlobalScope() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(true);
        return apiId;
    }

    private static ApiId apiIdForBillingFamily() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(false);
        when(apiId.isGroup()).thenReturn(true);
        final CachedGroupInfo groupInfo = mock(CachedGroupInfo.class);
        when(groupInfo.getEntityTypes()).thenReturn(Collections.emptySet());
        when(groupInfo.getGroupType()).thenReturn(GroupType.BILLING_FAMILY);
        when(groupInfo.getEntityIds()).thenReturn(ImmutableSet.of(OID_ACCOUNT_1, OID_ACCOUNT_2));
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(groupInfo));
        return apiId;
    }

    private static ApiId apiIdForGroupOfBillingFamily() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(false);
        when(apiId.isGroup()).thenReturn(true);
        final CachedGroupInfo groupInfo = mock(CachedGroupInfo.class);
        when(groupInfo.getEntityTypes()).thenReturn(Collections.emptySet());
        when(groupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(groupInfo.getNestedGroupTypes()).thenReturn(ImmutableSet.of(GroupType.BILLING_FAMILY));
        when(groupInfo.getEntityIds()).thenReturn(ImmutableSet.of(OID_ACCOUNT_1, OID_ACCOUNT_2));
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(groupInfo));
        return apiId;
    }

    private static ApiId apiIdForAccount() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(OID_ACCOUNT_1);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        final Set<ApiEntityType> scopeTypes = ImmutableSet.of(ApiEntityType.BUSINESS_ACCOUNT);
        when(apiId.getScopeTypes()).thenReturn(Optional.of(scopeTypes));
        when(apiId.isGroup()).thenReturn(false);
        return apiId;
    }

    private static ApiId apiIdForRegion() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(OID_REGION_1);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        final Set<ApiEntityType> scopeTypes = ImmutableSet.of(ApiEntityType.REGION);
        when(apiId.getScopeTypes()).thenReturn(Optional.of(scopeTypes));
        when(apiId.isGroup()).thenReturn(false);
        return apiId;
    }

    private static ApiId apiIdForRegionGroup() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(false);
        when(apiId.isGroup()).thenReturn(true);
        final CachedGroupInfo groupInfo = mock(CachedGroupInfo.class);
        when(groupInfo.getEntityTypes()).thenReturn(ImmutableSet.of(ApiEntityType.REGION));
        when(groupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(groupInfo.getEntityIds()).thenReturn(ImmutableSet.of(OID_REGION_1, OID_REGION_2));
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(groupInfo));
        return apiId;
    }

    private static ApiId apiIdForServiceProviderScope() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(OID_SERVICE_PROVIDER_1);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(true);
        final Set<ApiEntityType> scopeTypes = ImmutableSet.of(ApiEntityType.SERVICE_PROVIDER);
        when(apiId.getScopeTypes()).thenReturn(Optional.of(scopeTypes));
        when(apiId.isGroup()).thenReturn(false);
        return apiId;
    }

    private static ApiId apiIdForServiceProviderScopeGroup() {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.isEntity()).thenReturn(false);
        when(apiId.isGroup()).thenReturn(true);
        final CachedGroupInfo groupInfo = mock(CachedGroupInfo.class);
        when(groupInfo.getEntityTypes()).thenReturn(ImmutableSet.of(ApiEntityType.SERVICE_PROVIDER));
        when(groupInfo.getGroupType()).thenReturn(GroupType.REGULAR);
        when(groupInfo.getEntityIds()).thenReturn(ImmutableSet.of(OID_SERVICE_PROVIDER_1,
                        OID_SERVICE_PROVIDER_2));
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.of(groupInfo));
        return apiId;
    }

}
