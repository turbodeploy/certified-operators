package com.vmturbo.group.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.jooq.Condition;
import org.jooq.SelectWhereStep;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.group.db.Tables;

/**
 * A filter to restrict the {@link Group} objects to retrieve from the
 * {@link GroupStore}. It's closely tied to the grouping SQL table, and is
 * meant as a utility to provide an easier way to define simple searches
 * over the groups in the table.
 *
 * Conditions in the filter are applied by conjoining them together.
 */
@Immutable
public class GroupFilter {
    private final Set<Long> desiredIds;
    private final Set<Long> discoveredBy;

    private final List<Condition> conditions;

    public GroupFilter(final Set<Long> desiredIds,
                       final Set<Long> discoveredBy) {
        this.desiredIds = desiredIds;
        this.discoveredBy = discoveredBy;

        final List<Condition> conditions = new ArrayList<>();
        if (!desiredIds.isEmpty()) {
            conditions.add(Tables.GROUPING.ID.in(desiredIds));
        }
// TODO
//        if (!discoveredBy.isEmpty()) {
//            conditions.add(Tables.GROUPING.DISCOVERED_BY_ID.in(discoveredBy));
//        }

        this.conditions = Collections.unmodifiableList(conditions);
    }

    /**
     * Get the array of {@link Condition}s representing the conditions of
     * this filter. This can be passed into {@link SelectWhereStep#where(Condition...)}
     * when constructing the jOOQ query.
     *
     * @return The array of {@link Condition}s representing the filter.
     */
    public Condition[] getConditions() {
        return conditions.toArray(new Condition[conditions.size()]);
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Set<Long> ids = new HashSet<>();
        private Set<Long> discoveredBy = new HashSet<>();

        private Builder() {}

        public GroupFilter build() {
            return new GroupFilter(ids, discoveredBy);
        }

        @Nonnull
        public Builder addId(final long id) {
            this.ids.add(id);
            return this;
        }

        @Nonnull
        public Builder addIds(final Collection<Long> ids) {
            this.ids.addAll(ids);
            return this;
        }

        public Builder addDiscoveredBy(final long targetId) {
            this.discoveredBy.add(targetId);
            return this;
        }
    }
}
