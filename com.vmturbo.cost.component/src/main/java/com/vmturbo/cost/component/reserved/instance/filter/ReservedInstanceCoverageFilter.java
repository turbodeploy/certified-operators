package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.ArrayUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Table;

import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil;

/**
 * A filter to restrict the reserved instance coverage records from the
 * {@link com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageStore}.
 * It provider a easier way to define simple search over reserved instances coverage records
 * in the tables.
 */
public class ReservedInstanceCoverageFilter extends ReservedInstanceStatsFilter {

    private final EntityFilter entityFilter;
    private Long topologyContextId;

    private ReservedInstanceCoverageFilter(@Nonnull Builder builder) {
        super(builder);
        this.entityFilter = builder.entityFilter;
        this.topologyContextId = builder.topologyContextId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table<?> getTableName() {
        if (this.timeFrame == null || this.timeFrame.equals(TimeFrame.LATEST)) {
            return Tables.RESERVED_INSTANCE_COVERAGE_LATEST;
        } else if (this.timeFrame.equals(TimeFrame.HOUR)) {
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR;
        } else if (this.timeFrame.equals(TimeFrame.DAY)) {
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY;
        } else {
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Condition[] generateConditions(@Nonnull final DSLContext dslContext) {

        final Condition[] conditions = super.generateConditions(dslContext);

        if (entityFilter.getEntityIdCount() > 0 ) {
            final Table<?> table = getTableName();
            final Condition entityIdCondition = table.field(ReservedInstanceUtil.ENTITY_ID)
                    .in(entityFilter.getEntityIdList());
            return ArrayUtils.add(conditions, entityIdCondition);
        } else {
            return conditions;
        }
    }

    /**
     * Provides the OIDs for each underlying entity type-specific filter of this filter. The returned
     * OIDs will represent any region, account, availability zone, or entity filters configured
     * in this filter instance.
     * <p>
     * The returned oids can be used to scope RI coverage for entities through
     * the supply chain, in which the scoping information may not be readily available.
     * @return The scope OIDs of this filter or and empty list if this is a global filter. The order
     * of the list is irrelevant. The returned list is not modifiable.
     */
    public Collection<List<Long>> getStartingOidsPerScope() {
        return Stream.of(accountFilter.getAccountIdList(),
                entityFilter.getEntityIdList(), locationScopeStartingIds())
                .collect(Collectors.toSet());
    }

    private List<Long> locationScopeStartingIds() {
        return ImmutableList.<Long>builder()
                .addAll(regionFilter.getRegionIdList())
                .addAll(availabilityZoneFilter.getAvailabilityZoneIdList())
                .build();
    }

    public ReservedInstanceCoverageFilter toLatestFilter() {
        return ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(regionFilter)
                .availabilityZoneFilter(availabilityZoneFilter)
                .accountFilter(accountFilter)
                .entityFilter(entityFilter)
                .timeFrame(TimeFrame.LATEST)
                .build();
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static ReservedInstanceCoverageFilter.Builder newBuilder() {
        return new ReservedInstanceCoverageFilter.Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceCoverageFilter}
     */
    public static class Builder extends
            ReservedInstanceStatsFilter.Builder<ReservedInstanceCoverageFilter, Builder> {

        private EntityFilter entityFilter = EntityFilter.getDefaultInstance();
        private Long topologyContextId;

        /**
         * Set an entity filter, filtering the queried entity coverage by entity OID.
         * @param entityFilter The target {@link EntityFilter}, or null if no filtering by entity
         *                     OID is requested.
         * @return The {@link Builder} for method chaining
         */
        public Builder entityFilter(@Nullable EntityFilter entityFilter) {
            this.entityFilter = Optional.ofNullable(entityFilter)
                    .orElseGet(EntityFilter::getDefaultInstance);
            return this;
        }

        /**
         * The topology context for which RI coverage is requested
         *
         * @param topologyContextId the numeric ID of the topology for which RI coverage is requested
         * @return the Builder object
         */
        public Builder topologyContextId(Long topologyContextId) {
            this.topologyContextId = topologyContextId;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ReservedInstanceCoverageFilter build() {
            return new ReservedInstanceCoverageFilter(this);
        }
    }
}
