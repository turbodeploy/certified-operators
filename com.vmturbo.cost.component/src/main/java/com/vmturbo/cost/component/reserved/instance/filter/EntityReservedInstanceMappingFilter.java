package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Condition;

import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A filter used to extract data for a scoped set of Reserved Instance oids from EntityToReservedInstance
 * mapping.
 */
public class EntityReservedInstanceMappingFilter extends ReservedInstanceFilter {

    private final List<Condition> conditions;

    private EntityReservedInstanceMappingFilter(@Nonnull final Set<Long> scopeIds, int entityType) {
        super(scopeIds, Optional.of(entityType));
        this.conditions = generateConditions(scopeIds, Optional.of(entityType));
    }

    /**
     * Get the array of {@link Condition} representing the conditions of this filter.
     *
     * @return The array of {@link Condition} representing the filter.
     */
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    @Override
    List<Condition> generateConditions(@Nonnull Set<Long> scopeIds, Optional<Integer> scopeEntityType) {
        final List<Condition> conditions = new ArrayList<>();
        if (scopeIds.isEmpty()) {
            return conditions;
        }
        if (scopeEntityType.isPresent() && scopeEntityType.get() == EntityType.VIRTUAL_MACHINE_VALUE) {
            conditions.add(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID.in(scopeIds));
        } else {
            conditions.add(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID.in(scopeIds));
        }
        return conditions;
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder class to construct a filter.
     */
    public static class Builder {
        // The set of scope oids.
        private Set<Long> scopeIds = new HashSet<>();
        private int entityType;

        private Builder() {}

        /**
         * Build the required filter object of type EntityReservedInstanceMappingFilter.
         *
         * @return an object of type EntityReservedInstanceMappingFilter.
         */
        public EntityReservedInstanceMappingFilter build() {
            return new EntityReservedInstanceMappingFilter(scopeIds, entityType);
        }

        /**
         * Add all scope ids that are part of the requested riOids.
         *
         * @param ids The scope oids that represent the filtering conditions.
         * @return Builder for this class.
         */
        @Nonnull
        public Builder addAllScopeId(final List<Long> ids) {
            this.scopeIds.addAll(ids);
            return this;
        }

        /**
         * Add entity type for the Uuids being sent in scopeIds.
         * @param entityType - Entity Type number value.
         * @return Builder for this class.
         */
        @Nonnull
        public Builder addEntityType(int entityType) {
            this.entityType = entityType;
            return this;
        }
    }
}
