package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
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
     * @param scopeIds
     *     The scope(s) ids. Region/BusinessAccount/AvalilabilityZone etc.
     * @param scopeEntityType
     *     The scopes' entity type. In general, this ScopeIds is homegeneous and the scopeEntityType
     *     determines the type of entities contained in it. Exceptions are mixed groups including
     *     ResourceGroups for which a new scope entity type would likely be needed.
     */
    public ReservedInstanceFilter(@Nonnull final Set<Long> scopeIds,
                                  final int scopeEntityType) {
        this.scopeIds = ImmutableSet.copyOf(Objects.requireNonNull(scopeIds));
        this.scopeEntityType = scopeEntityType;
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

    /**
     * For access to scopeIds outside child classes.
     *
     * @return an immutable copy of the scopeIds Set.
     */
    @Nonnull
    public List<Long> getScopeIds() {
        return ImmutableList.copyOf(scopeIds);
    }

    /**
     * For access to entityType outside child classes.
     *
     * @return The entityType.
     */
    public int getEntityType() {
        return scopeEntityType;
    }
}
