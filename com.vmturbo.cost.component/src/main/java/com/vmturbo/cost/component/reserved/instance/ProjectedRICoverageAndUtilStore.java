package com.vmturbo.cost.component.reserved.instance;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Storage for projected reserved instance(RI) coverage of entities. For now we store them in a
 * simple map, because we only need the most recent snapshot and don't need aggregation for it.
 */
@ThreadSafe
public class ProjectedRICoverageAndUtilStore {
    private final Logger logger = LogManager.getLogger();

    /**
     * A map with key: VM/DB OID; value: A map with key: RI_ID; value: Coupons_Covered_By_RI.
     */
    private Map<Long, Map<Long, Double>> projectedEntityRICoverageMap = Collections.emptyMap();

    private final RepositoryClient repositoryClient;

    private final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub;

    // This should be the same as realtimeTopologyContextId.
    private long topologyContextId;
    private long realtimeTopologyContextId;

    // so we update all the information before any other access to the information
    private final Object lockObject = new Object();

    /**
     * Constructor that takes references to use to get information about requested scope.
     *
     * @param realtimeTopologyContextId
     *     The real time topology context ID
     * @param repositoryClient
     *     The repository client to access the scope information
     * @param supplyChainServiceBlockingStub
     *     The supply chain service blocking stub to pass to the scope processing
     */
    public ProjectedRICoverageAndUtilStore(
                    long realtimeTopologyContextId,
                    @Nonnull RepositoryClient repositoryClient,
                    @Nonnull SupplyChainServiceBlockingStub supplyChainServiceBlockingStub) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.supplyChainServiceBlockingStub =
                        Objects.requireNonNull(supplyChainServiceBlockingStub);
    }

    /**
     * Update the real time projected entity RI coverage in the store.
     *
     * @param originalTopologyInfo
     *     The information about the topology used to generate the Coverage, currently unused.
     * @param entityRICoverage
     *     A stream of the new {@link EntityReservedInstanceCoverage}. These will completely replace
     *     the existing entity RI coverage info.
     */
    public void updateProjectedRICoverage(
                    @Nonnull final TopologyInfo originalTopologyInfo,
                    @Nonnull final List<EntityReservedInstanceCoverage> entityRICoverage) {
        synchronized (lockObject) {
            Objects.requireNonNull(originalTopologyInfo, "topology info must not be null");
            topologyContextId = originalTopologyInfo.getTopologyContextId();
            final Map<Long, Map<Long, Double>> newCostsByEntity = entityRICoverage.stream()
                            .collect(Collectors.toMap(EntityReservedInstanceCoverage::getEntityId,
                                            EntityReservedInstanceCoverage::getCouponsCoveredByRiMap));
            projectedEntityRICoverageMap = Collections.unmodifiableMap(newCostsByEntity);
            //TODO: remove this logger or convert to debug
            logger.info("updateProjectedRICoverage topology ID {}, context {} , type {}, size {}",
                            originalTopologyInfo.getTopologyId(),
                            originalTopologyInfo.getTopologyContextId(),
                            originalTopologyInfo.getTopologyType(),
                            projectedEntityRICoverageMap.size());
        }
    }

    /**
     * Get the Reserved Instance Coverage Map, which has VM or DB OID key to value which is a Map of
     * RI ID to Coupons covered by that RI.
     *
     * @return A map with key: VM/DB OID; value: A map with key: RI_ID; value: Coupons_Covered_By_RI
     */
    @Nonnull
    public Map<Long, Map<Long, Double>> getAllProjectedEntitiesRICoverages() {
        synchronized (lockObject) {
            return projectedEntityRICoverageMap;
        }
    }

    /**
     * Get the Reserved Instance Coverage map for VMs and DBs in the scope defined by the filter. We
     * ask the repository for the collection of EntityTypes and the OIDs of each of those entity
     * types that are in the scope defined by using the filter's set of entity OIDs as the seed.
     *
     * @param filter
     *     The information about the scope to use to filter the Coverage Map
     * @return A map with key: VM/DB OID; value: A map with key: RI_ID; value: Coupons_Covered_By_RI
     */
    @Nonnull
    public Map<Long, Map<Long, Double>>
                    getScopedProjectedEntitiesRICoverages(ReservedInstanceFilter filter) {
        // Do the RPC before getting the lock, since the RPC can take a long time.
        List<Long> scopeIds = filter.getScopeIds();
        // getEntityOidsByType gets all entities in the real time topology if scopeIds is empty.
        Map<EntityType, Set<Long>> scopeMap = repositoryClient.getEntityOidsByType(scopeIds,
                        topologyContextId, supplyChainServiceBlockingStub);
        // this may return null if there are no VMs in scope, check below.
        Set<Long> scopedOids = scopeMap.getOrDefault(EntityType.VIRTUAL_MACHINE, Collections.emptySet());
        //TODO: add support for database VMs, make sure DATABASE is correct EntityType for them
        //scopedOids.addAll(scopeMap.get(EntityType.DATABASE));
        Map<Long, Map<Long, Double>> filteredMap = new HashMap<>();
        synchronized (lockObject) {
            if (scopedOids == null) {
                logger.debug("projectedEntityRICoverageMap.size() {}, scopeIds.size() {}"
                                + ", no entities found in scope",
                                projectedEntityRICoverageMap::size, scopeIds::size);
                return filteredMap;
            }
            logger.debug("projectedEntityRICoverageMap.size() {}, scopeIds.size() {}"
                            + ", scopedOids.size() {}", projectedEntityRICoverageMap::size,
                            scopeIds::size, scopedOids::size);
            for (Long anOid : scopedOids) {
                Map<Long, Double> value = projectedEntityRICoverageMap.get(anOid);
                if (value != null) {
                    filteredMap.put(anOid, value);
                    logger.info("For VM OID {} found projected coverage {}", anOid, value);
                } else {
                    logger.info("For VM OID {} no projected coverage found", anOid);
                }
            }
            return filteredMap;
        }
    }
}
