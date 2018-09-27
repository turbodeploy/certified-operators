package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Condition;

/**
 * A abstract class represents the filter object which will be used to query reserved instance tables.
 */
public abstract class ReservedInstanceFilter {

    protected final Set<Long> regionIds;

    protected final Set<Long> availabilityZoneIds;

    protected final Set<Long> businessAccountIds;

    public ReservedInstanceFilter(@Nonnull final Set<Long> regionIds,
                                       @Nonnull final Set<Long> availabilityZoneIds,
                                       @Nonnull final Set<Long> businessAccountIds) {
        this.regionIds = Objects.requireNonNull(regionIds);
        this.availabilityZoneIds = Objects.requireNonNull(availabilityZoneIds);
        this.businessAccountIds = Objects.requireNonNull(businessAccountIds);
    }

    abstract List<Condition> generateConditions(@Nonnull final Set<Long> regionIds,
                                                @Nonnull final Set<Long> availabilityZoneIds,
                                                @Nonnull final Set<Long> businessAccountIds);
}
