package com.vmturbo.api.component.external.api.util;

import static com.vmturbo.common.protobuf.action.ActionDTO.ActionType.BUY_RI;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.api.component.external.api.mapper.ActionTypeMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * This class handles specific logic applied to Buy RI scope. It serves the following goals:
 * <ol>
 * <li>Define whether Buy RI actions/discount are applicable to the selected scope.</li>
 * <li>Extract entities to be used to request Buy RI actions based on the selected scope.</li>
 * <li>Get action type list for action request considering whether Buy RI must be included.</li>
 * </ol>
 */
public class BuyRiScopeHandler {

    private static final Set<ApiEntityType> GROUP_OF_REGIONS = Collections.singleton(ApiEntityType.REGION);
    private static final Set<GroupType> GROUP_OF_BILLING_FAMILY = Collections.singleton(GroupType.BILLING_FAMILY);
    private static final Set<ApiEntityType> GROUP_OF_SERVICE_PROVIDERS = Collections.singleton(ApiEntityType.SERVICE_PROVIDER);

    /**
     * Extract action types from user input and selected scope. Selected scope affects whether
     * Buy RI actions should be included in the result. If Buy RIs are not applicable to the
     * selected scope they are filtered out even though they have been added to the user input.
     * Buy RI actions are included when the selected entity is a plan.
     *
     * @param inputDto User input with a list of requested actions type.
     * @param inputScope Selected entity.
     * @return Set of actions extracted from user input and selected scope.
     */
    public Set<ActionType> extractActionTypes(
            @Nonnull final ActionApiInputDTO inputDto,
            @Nullable final ApiId inputScope) {
        final Set<ActionType> actionTypes;
        final Set<ActionDTO.ActionType> actionTypesFromInput =
                CollectionUtils.emptyIfNull(inputDto.getActionTypeList()).stream()
                        .map(ActionTypeMapper::fromApi)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        if (inputScope != null && shouldIncludeBuyRiDiscount(inputScope)) {
            // If Buy RI actions must be included then use action types from input
            actionTypes = actionTypesFromInput;
        } else {
            // Real-time: Exclude Buy RI actions from input if input is provided or enumerate all action
            // types except of Buy RI if input is not provided
            final Stream<ActionType> actionTypesToFilter = actionTypesFromInput.isEmpty()
                    ? Stream.of(ActionDTO.ActionType.values())
                    : actionTypesFromInput.stream();
            actionTypes = actionTypesToFilter
                    .filter(actionType -> actionType != BUY_RI)
                    .collect(Collectors.toSet());
            // If user input contains a single Buy RI type and Buy RIs are not applicable to the
            // selected scope, we need to force empty result. It is achieved by setting NONE action
            // type. If we returned empty set instead result would include all action types which
            // would be wrong.
            if (actionTypes.isEmpty()) {
                return ImmutableSet.of(ActionType.NONE);
            }
        }
        return actionTypes;
    }

    /**
     * Extract entities related to Buy RI actions (regions, accounts) from selected scope.
     *
     * @param scopeId Selected scope.
     * @return Set of Buy RI related entities OIDs.
     */
    public Set<Long> extractBuyRiEntities(@Nullable final ApiId scopeId) {
        // Global scope
        if (scopeId == null || scopeId.isRealtimeMarket() || scopeId.isPlan()) {
            return Collections.emptySet();
        }

        // Entity scope (single Region)
        if (scopeId.isEntity() && scopeId.getScopeTypes().isPresent()) {
            if (GROUP_OF_REGIONS.equals(scopeId.getScopeTypes().orElse(null))) {
                return ImmutableSet.of(scopeId.oid());
            }
            return Collections.emptySet();
        }

        // Group scope (Billing Family, group of Regions or group of Billing Family)
        if (scopeId.isGroup() && scopeId.getCachedGroupInfo().isPresent()) {
            final UuidMapper.CachedGroupInfo groupInfo = scopeId.getCachedGroupInfo().get();

            // Group of regions
            if (GROUP_OF_REGIONS.equals(groupInfo.getEntityTypes())
                    // Billing Family
                    || groupInfo.getGroupType() == GroupType.BILLING_FAMILY
                    // Group of Billing Family
                    || GROUP_OF_BILLING_FAMILY.equals(groupInfo.getNestedGroupTypes())) {
                return groupInfo.getEntityIds();
            }
        }

        return Collections.emptySet();
    }

    /**
     * Determines if the buy RI discount should be included in the costs or actions queried for the
     * input scope.
     *
     * @param inputScope the input scope.
     * @return true if the RI buy should be included, false otherwise.
     */
    public boolean shouldIncludeBuyRiDiscount(@Nonnull final ApiId inputScope) {
        //only allow non-scoped-observer users.
        if (UserScopeUtils.isUserObserver() && UserScopeUtils.isUserScoped()) {
            return false;
        }
        // The buy RI discount should be shown in the realtime market (global) scope and plans.
        if (inputScope.isRealtimeMarket() || inputScope.isPlan()) {
            return true;
        } else if (inputScope.isEntity() && inputScope.getScopeTypes().isPresent()) {
            // The buy RI discount should be shown in scope of a region and a scoped based on service providers
            return GROUP_OF_REGIONS.equals(inputScope.getScopeTypes().orElse(null)) ||
                            GROUP_OF_SERVICE_PROVIDERS.equals(inputScope.getScopeTypes().orElse(null));
        } else if (inputScope.isGroup() && inputScope.getCachedGroupInfo().isPresent()) {
            final UuidMapper.CachedGroupInfo groupInfo = inputScope.getCachedGroupInfo().get();

            // If it is a group of region we should not exclude the buy RI discount
            if (GROUP_OF_REGIONS.equals(groupInfo.getEntityTypes())) {
                return true;
            }
            // Otherwise only return true if this is a billing family or group of billing family
            return groupInfo.getGroupType() == GroupType.BILLING_FAMILY
                    || GROUP_OF_BILLING_FAMILY.equals(groupInfo.getNestedGroupTypes());
        }
        return false;
    }
}
