package com.vmturbo.action.orchestrator.store.query;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;

/**
 * A simple map-backed implementation of {@link QueryableActionViews}. Useful for testing
 * and as a default fall-back.
 */
public class MapBackedActionViews implements QueryableActionViews {

    /**
     * (action id) -> ({@link ActionView}) for the action.
     * The actions could be of any type (buy RI, market, etc).
     */
    private final Map<Long, ActionView> actions;

    private final Predicate<ActionView> visibilityPredicate;

    public MapBackedActionViews(@Nonnull final Map<Long, ActionView> actions) {
        this(actions, actionView -> true);
    }

    public MapBackedActionViews(@Nonnull final Map<Long, ActionView> actions,
                                @Nonnull final Predicate<ActionView> visibilityPredicate) {
        this.actions = actions;
        this.visibilityPredicate = visibilityPredicate;
    }

    @Override
    public Stream<ActionView> get(@Nonnull final Collection<Long> actionIds) {
        return actionIds.stream()
            .map(actions::get)
            .filter(Objects::nonNull);
    }

    @Override
    public Optional<ActionView> get(final long actionId) {
        return Optional.ofNullable(actions.get(actionId));
    }

    @Override
    public Stream<ActionView> get(@Nonnull final ActionQueryFilter actionQueryFilter) {
        final QueryFilter queryFilter = new QueryFilter(actionQueryFilter, visibilityPredicate);
        return actions.values().stream()
            .filter(queryFilter::test);
    }

    @Override
    public Stream<ActionView> getByEntity(@Nonnull final Collection<Long> involvedEntities) {
        return get(ActionQueryFilter.newBuilder()
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addAllOids(involvedEntities)
                .build())
            .build());
    }

    @Override
    public Stream<ActionView> getAll() {
        final Map<Long, ActionView> actionViews = Collections.unmodifiableMap(actions);
        return actionViews.values().stream();
    }

    @Override
    public boolean isEmpty() {
        return actions.isEmpty();
    }
}
