package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Condition;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;

/**
 * A filter to restrict the {@link ReservedInstanceBought} from the {@link ReservedInstanceBoughtStore}.
 * It provider a easier way to define simple search over reserved instances records in the tables.
 */
public class ReservedInstanceBoughtFilter extends ReservedInstanceFilter {

    private final List<Condition> conditions;

    // Needs to set to true, if any filter needs to get from reserved instance spec table.
    private final boolean joinWithSpecTable;

    private ReservedInstanceBoughtFilter(@Nonnull final Set<Long> regionIds,
                                        @Nonnull final Set<Long> availabilityZoneIds,
                                        @Nonnull final Set<Long> businessAccountIds,
                                        final boolean joinWithSpecTable) {
        super(regionIds, availabilityZoneIds, businessAccountIds);
        this.joinWithSpecTable = joinWithSpecTable;
        this.conditions = generateConditions(regionIds, availabilityZoneIds, businessAccountIds);
    }

    /**
     * Get the array of {@link Condition} representing the conditions of this filter.
     *
     * @return The array of {@link Condition} representing the filter.
     */
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    public boolean isJoinWithSpecTable() {
        return this.joinWithSpecTable;
    }

    @Override
    protected List<Condition> generateConditions(@Nonnull final Set<Long> regionIds,
                                               @Nonnull final Set<Long> availabilityZoneIds,
                                               @Nonnull final Set<Long> businessAccountIds) {
        final List<Condition> conditions = new ArrayList<>();
        if (!regionIds.isEmpty()) {
            conditions.add(Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(regionIds));
        }

        if (!availabilityZoneIds.isEmpty()) {
            conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(availabilityZoneIds));
        }

        if (!businessAccountIds.isEmpty()) {
            conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(businessAccountIds));
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

    public static class Builder {
        private Set<Long> regionIds = new HashSet<>();
        private Set<Long> availabilityZoneIds = new HashSet<>();
        private Set<Long> businessAccountIds = new HashSet<>();
        private boolean joinWithSpecTable = false;

        private Builder() {}

        public ReservedInstanceBoughtFilter build() {
            return new ReservedInstanceBoughtFilter(regionIds, availabilityZoneIds, businessAccountIds,
                    joinWithSpecTable);
        }

        @Nonnull
        public Builder addRegionId(final long id) {
            this.regionIds.add(id);
            return this;
        }

        @Nonnull
        public Builder addAvailabilityZoneId(final long id) {
            this.availabilityZoneIds.add(id);
            return this;
        }

        @Nonnull
        public Builder addBusinessAccountId(final long id) {
            this.businessAccountIds.add(id);
            return this;
        }

        @Nonnull
        public Builder setJoinWithSpecTable(final boolean isJoinWithSpecTable) {
            this.joinWithSpecTable = isJoinWithSpecTable;
            return this;
        }
    }

}
