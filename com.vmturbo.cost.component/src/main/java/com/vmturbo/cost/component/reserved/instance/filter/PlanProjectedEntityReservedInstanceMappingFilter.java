package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.cost.component.db.Tables;

public class PlanProjectedEntityReservedInstanceMappingFilter extends EntityReservedInstanceMappingFilter {
    /**
     * The topology context ID corresponding to the plan in question
     */
    private final Long topologyContextId;

    private PlanProjectedEntityReservedInstanceMappingFilter(@Nonnull Builder builder) {
        this.topologyContextId = builder.topologyContextId;
        this.entityFilter = builder.entityFilter;
        this.riBoughtFilter = builder.riBoughtFilter;
        this.conditions = generateConditions();
    }

    /**
     * The getter for topologyContextId.
     *
     * @return the topologyContextId
     */
    public Long getTopologyContextId() {
        return topologyContextId;
    }

    /**
     * Generate conditions to be added to the WHERE clause of this DB query.
     *
     * @return a list of {@link Condition}s to be used by the corresponding JOOQ query
     */
    List<Condition> generateConditions() {
        final List<Condition> conditions = new ArrayList<>();
        // query plan projected context- instead of interrogating ENTITY_TO_RESERVED_INSTANCE_MAPPING, select
        // records from PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING, and filter on plan_id == topologyContextId
        if (entityFilter.getEntityIdCount() > 0) {
            conditions.add(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID
                    .in(entityFilter.getEntityIdList()));
        }

        if (riBoughtFilter.getRiBoughtIdCount() > 0) {
            if (riBoughtFilter.getExclusionFilter()) {
                conditions.add(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID
                        .notIn(riBoughtFilter.getRiBoughtIdList()));
            } else {
                conditions.add(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID
                        .in(riBoughtFilter.getRiBoughtIdList()));
            }
        }
        conditions.add(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.PLAN_ID
                .eq(topologyContextId));

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
    public static class Builder extends EntityReservedInstanceMappingFilter.Builder {
        private Long topologyContextId = null;

        @Nonnull
        public PlanProjectedEntityReservedInstanceMappingFilter.Builder topologyContextId(Long topologyContextId) {
            this.topologyContextId = topologyContextId;
            return this;
        }

        @Nonnull
        public PlanProjectedEntityReservedInstanceMappingFilter.Builder entityFilter(@Nullable Cost.EntityFilter entityFilter) {
            super.entityFilter(entityFilter);
            return this;
        }

        @Nonnull
        public Builder riBoughtFilter(@Nullable Cost.ReservedInstanceBoughtFilter riBoughtFilter) {
            super.riBoughtFilter(riBoughtFilter);
            return this;
        }

        @Nonnull
        public PlanProjectedEntityReservedInstanceMappingFilter build() {
            return new PlanProjectedEntityReservedInstanceMappingFilter(this);
        }

    }
}
