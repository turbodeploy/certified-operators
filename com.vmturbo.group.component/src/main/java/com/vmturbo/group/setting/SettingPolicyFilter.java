package com.vmturbo.group.setting;

import static com.vmturbo.group.db.Tables.SETTING_POLICY;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.jooq.Condition;
import org.jooq.SelectWhereStep;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;

/**
 * A filter to restrict the {@link SettingPolicy} objects to retrieve from the
 * {@link SettingStore}. It's closely tied to the setting_policy SQL table, and is
 * meant as a utility to provide an easier way to define simple searches
 * over the policies in the table.
 *
 * Conditions in the filter are applied by AND-ing them together.
 */
@Immutable
public class SettingPolicyFilter {
    private final Set<Type> desiredTypes;
    private final Set<String> desiredNames;
    private final Set<Long> desiredIds;
    private final Set<Long> desiredTargetIds;
    private final Set<Integer> desiredEntityTypes;

    /**
     * The pre-computed jOOQ conditions representing the filter.
     */
    private final List<Condition> conditions;

    private SettingPolicyFilter(@Nonnull final Set<Type> type,
                                @Nonnull final Set<String> name,
                                @Nonnull final Set<Long> ids,
                                @Nonnull final Set<Long> targetIds,
                                @Nonnull final Set<Integer> entityTypes) {
        this.desiredTypes = Objects.requireNonNull(type);
        this.desiredNames = Objects.requireNonNull(name);
        this.desiredIds = Objects.requireNonNull(ids);
        this.desiredTargetIds = Objects.requireNonNull(targetIds);
        this.desiredEntityTypes = Objects.requireNonNull(entityTypes);

        final ImmutableList.Builder<Condition> condBuilder = ImmutableList.builder();
        if (!type.isEmpty()) {
            condBuilder.add(SETTING_POLICY.POLICY_TYPE.in(type.stream()
                .map(SettingPolicyTypeConverter::typeToDb)
                .collect(Collectors.toSet())));
        }

        if (!name.isEmpty()) {
            condBuilder.add(SETTING_POLICY.NAME.in(name));
        }

        if (!ids.isEmpty()) {
            condBuilder.add(SETTING_POLICY.ID.in(ids));
        }

        if (!targetIds.isEmpty()) {
            condBuilder.add(SETTING_POLICY.TARGET_ID.in(targetIds));
        }

        if (!entityTypes.isEmpty()) {
            condBuilder.add(SETTING_POLICY.ENTITY_TYPE.in(entityTypes));
        }

        conditions = condBuilder.build();
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

    @Override
    public int hashCode() {
        return Objects.hash(desiredTypes, desiredNames, desiredIds, desiredTargetIds, desiredEntityTypes);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SettingPolicyFilter) {
            final SettingPolicyFilter otherFilter = (SettingPolicyFilter)other;
            return otherFilter.desiredTypes.equals(desiredTypes)
                && otherFilter.desiredNames.equals(desiredNames)
                && otherFilter.desiredIds.equals(desiredIds)
                && otherFilter.desiredTargetIds.equals(desiredTargetIds)
                && otherFilter.desiredEntityTypes.equals(desiredEntityTypes);
        } else {
            return false;
        }
    }

    /**
     * Builder for a {@link SettingPolicyFilter}.
     *
     * Multiple values on the same "with" condition are OR'ed together.
     * Multiple conditions on the filter are "AND'ed together.
     *
     * For example,
     * SettingPolicyFilter.newBuilder()
     *     .withType(Type.USER)
     *     .withType(Type.DEFAULT)
     *     .withName("foo")
     *     .withName("bar)
     *     .build()
     *
     * will create a filter that finds all USER or DEFAULT settings that ALSO have the name "foo" or "bar".
     *
     * An empty filter can be used to find all settings.
     */
    public static class Builder {
        private Set<Type> type = new HashSet<>();
        private Set<Long> ids = new HashSet<>();
        private Set<String> names = new HashSet<>();
        private Set<Long> targetIds = new HashSet<>();
        private Set<Integer> entityTypes = new HashSet<>();

        /**
         * Add a type that the filter will match. This method can be called
         * multiple times with different types.
         *
         * @param type The target type.
         * @return The builder, for chaining.
         */
        public Builder withType(@Nonnull final Type type) {
            this.type.add(type);
            return this;
        }

        /**
         * Add a setting policy name that the filter will match. This method can be called
         * multiple times with different names.
         *
         * @param name The target name.
         * @return The builder, for chaining.
         */
        public Builder withName(@Nonnull final String name) {
            this.names.add(name);
            return this;
        }

        /**
         * Add a setting policy id that the filter will match. This method can be called
         * multiple times with different ids.
         *
         * @param oid The target id.
         * @return The builder, for chaining.
         */
        public Builder withId(final long oid) {
            this.ids.add(oid);
            return this;
        }

        /**
         * Add a setting policy target id that the filter will match. This method can be called
         * multiple times with different ids.
         *
         * @param targetId The id of the target whose discovered setting policies should be matched.
         * @return The builder, for chaining.
         */
        public Builder withTargetId(final long targetId) {
            this.targetIds.add(targetId);
            return this;
        }

        /**
         * Add a setting policy entity type that the filter will match. This method can be called
         * multiple times with different entity types.
         *
         * @param entityType The entity type of the policy to match
         * @return The builder, for chaining.
         */
        public Builder withEntityType(final int entityType) {
            this.entityTypes.add(entityType);
            return this;
        }

        public SettingPolicyFilter build() {
            return new SettingPolicyFilter(type, names, ids, targetIds, entityTypes);
        }
    }
}
