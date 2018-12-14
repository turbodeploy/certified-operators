package com.vmturbo.cost.component.reserved.instance;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;

/**
 * Storage for projected reserved instance(RI) coverage of entities.
 *
 * For now we store them in a simple map, because we only need the most recent snapshot and don't
 * need aggregation for it.
 */
@ThreadSafe
public class ProjectedRICoverageStore {

    /**
     * A map of oid -> A map of <RI_ID, Coupons_Covered_By_RI>
     */
    private Map<Long, Map<Long, Double>> projectedEntityRICoverageMap = Collections.emptyMap();


    /**
     * Update the projected entity RI coverage in the store.
     *
     * @param entityRICoverage A stream of the new {@link EntityReservedInstanceCoverage}. These will completely replace
     *                    the existing entity RI coverage info.
     */
    public void updateProjectedRICoverage(@Nonnull final Stream<EntityReservedInstanceCoverage> entityRICoverage) {
        final Map<Long, Map<Long, Double>> newCostsByEntity =
            entityRICoverage.collect(Collectors.toMap(EntityReservedInstanceCoverage::getEntityId,
                        EntityReservedInstanceCoverage::getCouponsCoveredByRiMap));
            projectedEntityRICoverageMap = Collections.unmodifiableMap(newCostsByEntity);
    }

    @Nonnull
    public Map<Long, Map<Long, Double>> getAllProjectedEntitiesRICoverages() {
        return projectedEntityRICoverageMap;
    }
}
