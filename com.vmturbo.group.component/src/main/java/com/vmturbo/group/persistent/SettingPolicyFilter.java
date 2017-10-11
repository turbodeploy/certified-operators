package com.vmturbo.group.persistent;

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
 */
@Immutable
public class SettingPolicyFilter {
    private final Set<Type> desiredTypes;
    private final Set<String> desiredNames;
    private final Set<Long> desiredIds;

    /**
     * The pre-computed jOOQ conditions representing the filter.
     */
    private final List<Condition> conditions;

    private SettingPolicyFilter(@Nonnull final Set<Type> type,
                                @Nonnull final Set<String> name,
                                @Nonnull final Set<Long> ids) {
        this.desiredTypes = type;
        this.desiredNames = name;
        this.desiredIds = ids;

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
        return Objects.hash(desiredTypes, desiredNames, desiredIds);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SettingPolicyFilter) {
            final SettingPolicyFilter otherFilter = (SettingPolicyFilter)other;
            return otherFilter.desiredTypes.equals(desiredTypes)
                && otherFilter.desiredNames.equals(desiredNames)
                && otherFilter.desiredIds.equals(desiredIds);
        } else {
            return false;
        }
    }

    public static class Builder {
        private Set<Type> type = new HashSet<>();
        private Set<Long> ids = new HashSet<>();
        private Set<String> names = new HashSet<>();

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
         * multiple times with diferent names.
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
         * multiple times with diferent ids.
         *
         * @param oid The target id.
         * @return The builder, for chaining.
         */
        public Builder withId(final long oid) {
            this.ids.add(oid);
            return this;
        }

        public SettingPolicyFilter build() {
            return new SettingPolicyFilter(type, names, ids);
        }
    }
}
