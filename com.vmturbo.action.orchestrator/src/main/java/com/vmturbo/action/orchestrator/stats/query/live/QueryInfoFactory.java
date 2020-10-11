package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander.InvolvedEntitiesFilter;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.InvolvedEntityExpansionUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.ScopeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest.SingleQuery;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;

/**
 * Responsible for creating a {@link QueryInfo} from a {@link SingleQuery}
 * (i.e. a {@link com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery}).
 */
public class QueryInfoFactory {

    private static final Logger logger = LogManager.getLogger();

    private final long realtimeContextId;

    private final UserSessionContext userSessionContext;

    private final InvolvedEntitiesExpander involvedEntitiesExpander;

    /**
     * Constructs a factory that creates {@link QueryInfo} using realtimeContextId,
     * userSessionContext, and involvedEntitiesExpander.
     *
     * @param realtimeContextId the context id of real time.
     * @param userSessionContext info related to the user session.
     * @param involvedEntitiesExpander how involved entities should be expanded.
     */
    public QueryInfoFactory(
            final long realtimeContextId,
            final UserSessionContext userSessionContext,
            @Nonnull InvolvedEntitiesExpander involvedEntitiesExpander) {
        this.realtimeContextId = realtimeContextId;
        this.userSessionContext = userSessionContext;
        this.involvedEntitiesExpander = involvedEntitiesExpander;
    }

    private boolean isGlobalTempGroupOfExpansionRequiredEntityTypes(@Nonnull final SingleQuery query) {
        if (query.hasQuery() && query.getQuery().hasScopeFilter()
                && query.getQuery().getScopeFilter().hasGlobal()
                && !query.getQuery().getScopeFilter().getGlobal().getEntityTypeList().isEmpty()) {
            return query.getQuery().getScopeFilter().getGlobal().getEntityTypeList().stream()
                    .allMatch(InvolvedEntityExpansionUtil::expansionRequiredEntityType);
        }
        return false;
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
        final ImmutableQueryInfo.Builder immutableQueryInfo = ImmutableQueryInfo.builder();
        final Set<Long> desiredEntities;
        if (query.getQuery().hasScopeFilter()
                && query.getQuery().getScopeFilter().getScopeCase() == ScopeCase.ENTITY_LIST
                && query.getQuery().getScopeFilter().hasEntityList()) {
            List<Long> oids = query.getQuery().getScopeFilter().getEntityList().getOidsList();
            InvolvedEntitiesFilter filter =
                involvedEntitiesExpander.expandInvolvedEntitiesFilter(oids);
            desiredEntities = filter.getEntities();
            immutableQueryInfo.involvedEntityCalculation(filter.getCalculationType());
        } else if (isGlobalTempGroupOfExpansionRequiredEntityTypes(query)) {
            desiredEntities = null;
            immutableQueryInfo.involvedEntityCalculation(
                    InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
        } else {
            desiredEntities = null;
            immutableQueryInfo.involvedEntityCalculation(
                InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
        }
        immutableQueryInfo.desiredEntities(desiredEntities);

        final Predicate<ActionEntity> entityPredicate = query.getQuery().hasScopeFilter() ?
            createEntityPredicate(query.getQuery().getScopeFilter(), desiredEntities) : entity -> true;
        final Predicate<SingleActionInfo> actionGroupPredicate = query.getQuery().hasActionGroupFilter() ?
            createActionGroupPredicate(query.getQuery().getActionGroupFilter()) : entity -> true;

        final long topologyContextId;
        if (query.getQuery().getScopeFilter().hasTopologyContextId()) {
            topologyContextId = query.getQuery().getScopeFilter().getTopologyContextId();
        } else {
            topologyContextId = realtimeContextId;

        }

        return immutableQueryInfo.queryId(query.getQueryId())
            .topologyContextId(topologyContextId)
            .entityPredicate(entityPredicate)
            .query(query.getQuery())
            .actionGroupPredicate(actionGroupPredicate)
            .build();
    }

    /**
     * Creates the entity predicate depending on the scopeFilter.getScopeCase().
     *
     * @param scopeFilter the scope filter to create the entity predicate from.
     * @param desiredEntityIds is never null when scopeFilter.getScopeCase() == ENTITY_LIST
     * @return the entity predicate.
     */
    @Nonnull
    private Predicate<ActionEntity> createEntityPredicate(
            @Nonnull final ScopeFilter scopeFilter,
            @Nullable Set<Long> desiredEntityIds) {
        final Predicate<ActionEntity> entityPredicate;
        switch (scopeFilter.getScopeCase()) {
            case ENTITY_LIST:
                entityPredicate = actionEntity -> desiredEntityIds.contains(actionEntity.getId());
                break;
            case GLOBAL:
                final Predicate<ActionEntity> envTypePredicate;
                if (!scopeFilter.getGlobal().hasEnvironmentType()) {
                    envTypePredicate = entity -> true;
                } else {
                    // use standard environment matching for entities
                    // as implemented in EnvironmentTypeUtil
                    final EnvironmentType envTypeQuery = scopeFilter.getGlobal().getEnvironmentType();
                    envTypePredicate = entity -> EnvironmentTypeUtil.matchingPredicate(envTypeQuery)
                                                        .test(entity.getEnvironmentType());
                }
                final Set<Integer> desiredEntityTypes =
                    Sets.newHashSet(scopeFilter.getGlobal().getEntityTypeList());

                final Predicate<ActionEntity> entityTypePredicate;
                if (desiredEntityTypes.isEmpty()) {
                    entityTypePredicate = entity -> true;
                } else if (desiredEntityTypes.stream().anyMatch(InvolvedEntityExpansionUtil::expansionRequiredEntityType)) {
                    // If the scope is a global temp group of expansion-required entities, the
                    // predicate should be true for entities that should be propagated to
                    // expansion-required entity types.
                    entityTypePredicate = actionEntity ->
                            desiredEntityTypes.contains(actionEntity.getType())
                                    || involvedEntitiesExpander.shouldPropagateAction(
                                            actionEntity.getId(), desiredEntityTypes);
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
