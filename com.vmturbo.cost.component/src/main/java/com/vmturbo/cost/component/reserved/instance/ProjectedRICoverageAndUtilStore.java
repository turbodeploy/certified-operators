package com.vmturbo.cost.component.reserved.instance;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;

/**
 * Storage for projected reserved instance(RI) coverage of entities.
 *
 * For now we store them in a simple map, because we only need the most recent snapshot and don't
 * need aggregation for it.
 */
@ThreadSafe
public class ProjectedRICoverageAndUtilStore {

    /**
     * A map of oid -> A map of <RI_ID, Coupons_Covered_By_RI>
     */
    private Map<Long, Map<Long, Double>> projectedEntityRICoverageMap = Collections.emptyMap();

    private final RepositoryServiceBlockingStub repositoryClient;

    public ProjectedRICoverageAndUtilStore(@Nonnull RepositoryServiceBlockingStub repositoryClient) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
    }

    /**
     * Update the real time projected entity RI coverage in the store.
     *
     * @param entityRICoverage A stream of the new {@link EntityReservedInstanceCoverage}. These will completely replace
     *                    the existing entity RI coverage info.
     */
    public void updateProjectedRICoverage(@Nonnull final List<EntityReservedInstanceCoverage> entityRICoverage) {
        final Map<Long, Map<Long, Double>> newCostsByEntity =
            entityRICoverage.stream().collect(Collectors.toMap(EntityReservedInstanceCoverage::getEntityId,
                        EntityReservedInstanceCoverage::getCouponsCoveredByRiMap));
            projectedEntityRICoverageMap = Collections.unmodifiableMap(newCostsByEntity);
    }

    @Nonnull
    public Map<Long, Map<Long, Double>> getAllProjectedEntitiesRICoverages() {
        return projectedEntityRICoverageMap;
    }

}
