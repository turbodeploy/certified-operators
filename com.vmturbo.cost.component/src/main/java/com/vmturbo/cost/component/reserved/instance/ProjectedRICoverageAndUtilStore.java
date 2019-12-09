package com.vmturbo.cost.component.reserved.instance;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
     * A map with key: VM/DB OID; value: EntityReservedInstanceCoverage.
     */
    private Map<Long, EntityReservedInstanceCoverage> projectedEntitiesRICoverage = new HashMap<>();

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
     * @param entitiesRICoverage
     *     A stream of the new {@link EntityReservedInstanceCoverage}. These will completely replace
     *     the existing entity RI coverage info.
     */
    public void updateProjectedRICoverage(
                    @Nonnull final TopologyInfo originalTopologyInfo,
                    @Nonnull final List<EntityReservedInstanceCoverage> entitiesRICoverage) {
        synchronized (lockObject) {
            Objects.requireNonNull(originalTopologyInfo, "topology info must not be null");
            topologyContextId = originalTopologyInfo.getTopologyContextId();
            // Clear the coverage map before updating.
            projectedEntitiesRICoverage.clear();
            for (EntityReservedInstanceCoverage entityRICoverage : entitiesRICoverage) {
                projectedEntitiesRICoverage.put(entityRICoverage.getEntityId(), entityRICoverage);
            }
            //TODO: remove this logger or convert to debug
            logger.info("updateProjectedRICoverage topology ID {}, context {} , type {}, size {}",
                            originalTopologyInfo.getTopologyId(),
                            originalTopologyInfo.getTopologyContextId(),
                            originalTopologyInfo.getTopologyType(),
                    projectedEntitiesRICoverage.size());
        }
    }

    /**
     * Get the Reserved Instance Coverage Map, which has VM or DB OID key to value which is a Map of
     * RI ID to Coupons covered by that RI.
     *
     * @return A map with key: VM/DB OID; value: EntityReservedInstanceCoverage.
     */
    @Nonnull
    public Map<Long, EntityReservedInstanceCoverage> getAllProjectedEntitiesRICoverages() {
        synchronized (lockObject) {
            return Collections.unmodifiableMap(projectedEntitiesRICoverage);
        }
    }

    /**
     * Get the Reserved Instance Coverage map for VMs and DBs in the scope defined by the filter. We
     * ask the repository for the collection of EntityTypes and the OIDs of each of those entity
     * types that are in the scope defined by using the filter's set of entity OIDs as the seed.
     *
     * @param filter The information about the scope to use to filter the Coverage Map.
     * @return A map with key: RI_ID; value: EntityReservedInstanceCoverage.
     */
    @Nonnull
    public Map<Long, EntityReservedInstanceCoverage>
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

        synchronized (lockObject) {
            if (scopedOids == null) {
                logger.debug("projectedEntitiesRICoverage.size() {}, scopeIds.size() {}"
                                + ", no entities found in scope",
                                projectedEntitiesRICoverage::size, scopeIds::size);
                return Collections.EMPTY_MAP;
            }
            Map<Long, EntityReservedInstanceCoverage> filteredMap = new HashMap<>();
            logger.debug("projectedEntitiesRICoverage.size() {}, scopeIds.size() {}"
                            + ", scopedOids.size() {}", projectedEntitiesRICoverage::size,
                            scopeIds::size, scopedOids::size);
            for (Long anOid : scopedOids) {
                EntityReservedInstanceCoverage value = projectedEntitiesRICoverage.get(anOid);
                if (value != null) {
                    filteredMap.put(anOid, value);
                    logger.info("For VM OID {} found projected coverage {}", anOid, value);
                } else {
                    logger.info("For VM OID {} no projected coverage found", anOid);
                }
            }
            return Collections.unmodifiableMap(filteredMap);
        }
    }
}
