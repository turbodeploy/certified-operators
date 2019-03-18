package com.vmturbo.cost.component.reserved.instance;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.google.common.annotations.VisibleForTesting;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Storage for projected reserved instance(RI) coverage of entities.
 *
 * For now we store them in a simple map, because we only need the most recent snapshot and don't
 * need aggregation for it.
 */
@ThreadSafe
public class ProjectedRICoverageAndUtilStore {

    private final static Logger logger = LogManager.getLogger();
    /**
     * A map of oid -> A map of <RI_ID, Coupons_Covered_By_RI>
     */
    private Map<Long, Map<Long, Double>> projectedEntityRICoverageMap = Collections.emptyMap();

    private final RepositoryServiceBlockingStub repositoryClient;

    private final int chunkSize = 1000;

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
