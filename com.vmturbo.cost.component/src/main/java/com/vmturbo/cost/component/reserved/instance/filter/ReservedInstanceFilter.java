package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.jooq.Condition;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * A abstract class represents the filter object which will be used to query reserved instance tables.
 */
public abstract class ReservedInstanceFilter {

    protected final Set<Long> scopeIds;
    protected final int scopeEntityType;

    protected static final Set<Integer> SUPPORTED_RI_FILTER_TYPES =
        ImmutableSet.of(EntityDTO.EntityType.REGION_VALUE,
                        EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE,
                        EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE);

    /**
     * Constructor that takes scopeId(s) and scopeEntityType as arguments.
     *
     * @param scopeIds The scope(s) ids.  Region/BusinessAccount/AvalilabilityZone etc.
     * @param scopeEntityType  The scopes' enity type.
     */
    public ReservedInstanceFilter(@Nonnull final Set<Long> scopeIds,
                                  @Nonnull final int scopeEntityType) {
        this.scopeIds = Objects.requireNonNull(scopeIds);
        this.scopeEntityType = Objects.requireNonNull(scopeEntityType);
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @param scopeIds scope ids need to filter by.
     * @param scopeEntityType scope(s) entity type.
     * @return a list of {@link Condition}.
     */
    abstract List<Condition> generateConditions(@Nonnull Set<Long> scopeIds,
                                                int scopeEntityType);
}
