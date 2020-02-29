package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Responsible for creating a {@link QueryInfo} from a {@link SingleQuery}
 * (i.e. a {@link com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery}).
 */
public class QueryInfoFactory {
    private static final Logger logger = LogManager.getLogger();

    private final long realtimeContextId;

    private final UserSessionContext userSessionContext;

    public QueryInfoFactory(final long realtimeContextId, final UserSessionContext userSessionContext) {
        this.realtimeContextId = realtimeContextId;
        this.userSessionContext = userSessionContext;
    }

    /**
     * Extracts the {@link QueryInfo} from a {@link SingleQuery}, filling all the necessary
     * parameters.
     *
     * @param query The {@link SingleQuery} received from the caller.
     * @return A {@link QueryInfo}.
     */
    @Nonnull
    QueryInfo extractQueryInfo(@Nonnull final SingleQuery query) {
        final Predicate<ActionEntity> entityPredicate = query.getQuery().hasScopeFilter() ?
            createEntityPredicate(query.getQuery().getScopeFilter()) : entity -> true;
        final Predicate<SingleActionInfo> actionGroupPredicate = query.getQuery().hasActionGroupFilter() ?
            createActionGroupPredicate(query.getQuery().getActionGroupFilter()) : entity -> true;

        final long topologyContextId;
        if (query.getQuery().getScopeFilter().hasTopologyContextId()) {
            topologyContextId = query.getQuery().getScopeFilter().getTopologyContextId();
        } else {
            topologyContextId = realtimeContextId;

        }

        return ImmutableQueryInfo.builder()
            .queryId(query.getQueryId())
            .topologyContextId(topologyContextId)
            .entityPredicate(entityPredicate)
            .query(query.getQuery())
            .actionGroupPredicate(actionGroupPredicate)
            .build();
    }

    @Nonnull
    private Predicate<ActionEntity> createEntityPredicate(@Nonnull final ScopeFilter scopeFilter) {
        final Predicate<ActionEntity> entityPredicate;
        switch (scopeFilter.getScopeCase()) {
            case ENTITY_LIST:
                final Set<Long> desiredEntityIds =
                    Sets.newHashSet(scopeFilter.getEntityList().getOidsList());
                entityPredicate = actionEntity -> desiredEntityIds.contains(actionEntity.getId());
                break;
            case GLOBAL:
                final Predicate<ActionEntity> envTypePredicate =
                    scopeFilter.getGlobal().hasEnvironmentType() ?
                        entity -> {
                            // In the case of unknown environment type we default to on-prem.
                            final EnvironmentType entityEnv =
                                entity.getEnvironmentType() == EnvironmentType.CLOUD ?
                                    EnvironmentType.CLOUD : EnvironmentType.ON_PREM;
                            return entityEnv == scopeFilter.getGlobal().getEnvironmentType();
                        }: entity -> true;

                final Set<Integer> desiredEntityTypes =
                    Sets.newHashSet(scopeFilter.getGlobal().getEntityTypeList());

                final Predicate<ActionEntity> entityTypePredicate;
                if (desiredEntityTypes.isEmpty()) {
                    entityTypePredicate = entity -> true;
                } else {
                    // Note - we don't expect to hit this condition if the scope case is "entity list."
                    entityTypePredicate = actionEntity -> desiredEntityTypes.contains(actionEntity.getType());
                }
                // if the user is scoped, add that as a predicate too
                if (userSessionContext.isUserScoped()) {
                    EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
                    final Predicate<ActionEntity> userScopePredicate = actionEntity -> entityAccessScope.contains(actionEntity.getId());
                    entityPredicate = userScopePredicate.and(envTypePredicate.and(entityTypePredicate));
                } else {
                    entityPredicate = envTypePredicate.and(entityTypePredicate);
                }
                break;
            default:
                logger.warn("Unexpected scope {} for filter. Assuming \"market\" scope.",
                    scopeFilter.getScopeCase());
                entityPredicate = entity -> true;
        }

        return entityPredicate;
    }

    @Nonnull
    private Predicate<SingleActionInfo> createActionGroupPredicate(@Nonnull final ActionGroupFilter actionGroupFilter) {
        Predicate<SingleActionInfo> actionGroupPredicate = actionInfo -> true;
        if (!actionGroupFilter.getActionTypeList().isEmpty()) {
            actionGroupPredicate = actionGroupPredicate.and(actionInfo -> {
                // Note - using "contains" instead of constructing a new set, because the
                // input list is a single-digit size.
                return actionGroupFilter.getActionTypeList().contains(
                        ActionDTOUtil.getActionInfoActionType(
                                actionInfo.action().getTranslationResultOrOriginal()));
            });
        }
        if (!actionGroupFilter.getActionCategoryList().isEmpty()) {
            actionGroupPredicate = actionGroupPredicate.and(actionInfo -> {
                // Note - using "contains" instead of constructing a new set, because the
                // input list is a single-digit size.
                return actionGroupFilter.getActionCategoryList().contains(
                    actionInfo.action().getActionCategory());
            });
        }
        if (!actionGroupFilter.getActionStateList().isEmpty()) {
            actionGroupPredicate = actionGroupPredicate.and(actionInfo -> {
                // Note - using "contains" instead of constructing a new set, because the
                // input list is a single-digit size.
                return actionGroupFilter.getActionStateList().contains(
                    actionInfo.action().getState());
            });
        }
        if (!actionGroupFilter.getActionModeList().isEmpty()) {
            actionGroupPredicate = actionGroupPredicate.and(actionInfo -> {
                // Note - using "contains" instead of constructing a new set, because the
                // input list is a single-digit size.
                return actionGroupFilter.getActionModeList().contains(
                    actionInfo.action().getMode());
            });
        }
        final Boolean visible = actionGroupFilter.hasVisible()
                ? actionGroupFilter.getVisible()
                : null;
        actionGroupPredicate = actionGroupPredicate.and(actionInfo ->
                actionInfo.action().getVisibilityLevel().checkVisibility(visible));
        return actionGroupPredicate;
    }
}
