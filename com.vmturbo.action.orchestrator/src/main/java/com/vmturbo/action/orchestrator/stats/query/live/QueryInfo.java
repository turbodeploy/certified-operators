package com.vmturbo.action.orchestrator.stats.query.live;

import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;

/**
 * Wrapper around a {@link CurrentActionStatsQuery}.
 */
@Value.Immutable
public interface QueryInfo {
    /**
     * The id of the query. This is specified in the request, and needs to be present in the
     * response so that the caller knows which query the response is for.
     */
    Long queryId();

    /**
     * The topology context ID the query is targetting.
     */
    Long topologyContextId();

    /**
     * The query itself.
     */
    CurrentActionStatsQuery query();

    /**
     * The predicate to apply to the entities involved in actions in the target {@link ActionStore}.
     * Only actions that have at least one involved entity matching the predicate match the query.
     * Only involved entities that match the predicate are counted for the entity count
     * in the returned action stats.
     */
    Predicate<ActionEntity> entityPredicate();

    /**
     * The predicate to apply to the actions in the target {@link ActionStore}.
     * Only actions that match the predicate match the query.
     */
    Predicate<SingleActionInfo> actionGroupPredicate();

    /**
     * A combination of {@link QueryInfo#entityPredicate()} and
     * {@link QueryInfo#actionGroupPredicate()}.
     */
    @Nonnull
    default Predicate<SingleActionInfo> viewPredicate() {
        final Predicate<SingleActionInfo> scopeFilterPredicate = query().hasScopeFilter() ?
            actionInfo -> actionInfo.involvedEntities().stream().anyMatch(entityPredicate()) :
            actionInfo -> true;

        return scopeFilterPredicate.and(actionGroupPredicate());
    }
}
