package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter used to extract data for a scoped set of Reserved Instance oids from EntityToReservedInstance
 * mapping.
 */
public class EntityReservedInstanceMappingFilter {
    @Nullable
    protected EntityFilter entityFilter;

    @Nullable
    protected Cost.ReservedInstanceBoughtFilter riBoughtFilter;

    protected List<Condition> conditions;

    // Default constructor called before subclass constructor definition
    public EntityReservedInstanceMappingFilter() {
        this.entityFilter = null;
        this.riBoughtFilter = null;
        this.conditions = null;
    }

    private EntityReservedInstanceMappingFilter(@Nonnull Builder builder) {
        this.entityFilter = builder.entityFilter;
        this.riBoughtFilter = builder.riBoughtFilter;
        this.conditions = generateConditions();
    }

    /**
     * Get the array of {@link Condition} representing the conditions of this filter.
     *
     * @return The array of {@link Condition} representing the filter.
     */
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }
    List<Condition> generateConditions() {
        final List<Condition> conditions = new ArrayList<>();
        if (entityFilter.getEntityIdCount() > 0) {
            conditions.add(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID
                    .in(entityFilter.getEntityIdList()));
        }

        if (riBoughtFilter.getRiBoughtIdCount() > 0) {
            if (riBoughtFilter.getExclusionFilter()) {
                conditions.add(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID
                        .notIn(riBoughtFilter.getRiBoughtIdList()));
            } else {
                conditions.add(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID
                        .in(riBoughtFilter.getRiBoughtIdList()));
            }
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
        protected EntityFilter entityFilter =  EntityFilter.getDefaultInstance();
        protected Cost.ReservedInstanceBoughtFilter riBoughtFilter =
                Cost.ReservedInstanceBoughtFilter.getDefaultInstance();

        @Nonnull
        public Builder entityFilter(@Nullable EntityFilter entityFilter) {
            this.entityFilter = Optional.ofNullable(entityFilter)
                    .orElse(EntityFilter.getDefaultInstance());
            return this;
        }

        @Nonnull
        public Builder riBoughtFilter(@Nullable Cost.ReservedInstanceBoughtFilter riBoughtFilter) {
            this.riBoughtFilter = Optional.ofNullable(riBoughtFilter)
                    .orElse(Cost.ReservedInstanceBoughtFilter.getDefaultInstance());
            return this;
        }

        @Nonnull
        public EntityReservedInstanceMappingFilter build() {
            return new EntityReservedInstanceMappingFilter(this);
        }

    }
}
