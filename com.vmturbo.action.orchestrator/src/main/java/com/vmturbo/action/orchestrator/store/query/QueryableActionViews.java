package com.vmturbo.action.orchestrator.store.query;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;

/**
 * An interface to present the {@link ActionView}s in an
 * {@link com.vmturbo.action.orchestrator.store.ActionStore} in a way that allows various queries.
 * <p>
 * Specific {@link com.vmturbo.action.orchestrator.store.ActionStore} implementations can return
 * implementations of this interface that are optimized to handle various types of queries.
 */
public interface QueryableActionViews {

    /**
     * Get actions by ID. The multi-get version of {@link QueryableActionViews#get(long)}.
     *
     * @param actionIds The ids to retrieve.
     * @return A stream of {@link ActionView}s that match the ids.
     */
    Stream<ActionView> get(@Nonnull Collection<Long> actionIds);

    /**
     * Get an action view by ID.
     *
     * @param actionId The id of the action to retrieve.
     * @return An optional containing the {@link ActionView}, or an empty optional.
     */
    Optional<ActionView> get(final long actionId);

    /**
     * TODO (roman, May 23 2019): Port the stats queries ({@link ScopeFilter} and
     * {@link ActionGroupFilter}) here as well. Also, it's probably a good idea to
     * combine the stats queries interface and {@link ActionQueryFilter} so we just have one
     * way of specifying actions to target.
     *
     * @param actionQueryFilter The {@link ActionQueryFilter}.
     * @return A stream of {@link ActionView}s that match the filter.
     */
    Stream<ActionView> get(@Nonnull final ActionQueryFilter actionQueryFilter);

    /**
     * Get all actions that the provided entities are involved in.
     *
     * @param involvedEntities A collection of entity IDs.
     * @return A stream of {@link ActionView}s that involve any of the provided entities.
     *         The stream will NOT contain duplicate {@link ActionView}s.
     */
    Stream<ActionView> getByEntity(@Nonnull final Collection<Long> involvedEntities);

    /**
     * Get all actions.
     *
     * @return A stream of all {@link ActionView}s.
     */
    Stream<ActionView> getAll();

    /**
     * Return whether or not the collection of views is empty.
     */
    boolean isEmpty();
}
