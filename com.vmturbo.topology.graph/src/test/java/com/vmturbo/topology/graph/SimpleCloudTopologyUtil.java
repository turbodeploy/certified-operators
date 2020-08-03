package com.vmturbo.topology.graph;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.topology.graph.TestGraphEntity.Builder;

/**
 * <p>This utility class can be used to construct a simple cloud topology,
 * for the needs of some of the unit tests.</p>
 *
 * <p>REGION --owns-> ZONE --aggregates-> DB, VM
 * REGION --aggregates-> COMPUTE TIER, STORAGE TIER
 * DB --consumes-> VM --consumes-> COMPUTE TIER
 * COMPUTE TIER --connects_to-> STORAGE TIER</p>
 *
 * <p>ids: REGION -> 1, ZONE -> 2, DB -> 3, VM -> 4,
 *         COMPUTE TIER -> 5, STORAGE TIER -> 6</p>
 */
public class SimpleCloudTopologyUtil {
    /**
     * Ids of the entities of the topology.
     */
    public static final long RG_ID = 1L;
    public static final long AZ_ID = 2L;
    public static final long DB_ID = 3L;
    public static final long VM_ID = 4L;
    public static final long CT_ID = 5L;
    public static final long ST_ID = 6L;

    private SimpleCloudTopologyUtil() {}

    /**
     * Constructs the topology.
     *
     * <p>REGION --owns-> ZONE --aggregates-> DB, VM
     * REGION --aggregates-> COMPUTE TIER, STORAGE TIER
     * DB --consumes-> VM --consumes-> COMPUTE TIER
     * COMPUTE TIER --connects_to-> STORAGE TIER</p>
     *
     * <p>ids: REGION -> 1, ZONE -> 2, DB -> 3, VM -> 4,
     *         COMPUTE TIER -> 5, STORAGE TIER -> 6</p>
     *
     * @return the topology.
     */
    public static TopologyGraph<TestGraphEntity> constructTopology() {
        final TestGraphEntity.Builder region =
                TestGraphEntity.newBuilder(RG_ID, ApiEntityType.REGION)
                        .addConnectedEntity(AZ_ID, ConnectionType.OWNS_CONNECTION);
        final TestGraphEntity.Builder zone =
                TestGraphEntity.newBuilder(AZ_ID, ApiEntityType.AVAILABILITY_ZONE);
        final TestGraphEntity.Builder db =
                TestGraphEntity.newBuilder(DB_ID, ApiEntityType.DATABASE).addProviderId(VM_ID)
                        .addConnectedEntity(AZ_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
        final TestGraphEntity.Builder vm =
                TestGraphEntity.newBuilder(VM_ID, ApiEntityType.VIRTUAL_MACHINE).addProviderId(CT_ID)
                        .addConnectedEntity(AZ_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
        final TestGraphEntity.Builder computeTier =
                TestGraphEntity.newBuilder(CT_ID, ApiEntityType.COMPUTE_TIER)
                        .addConnectedEntity(ST_ID, ConnectionType.NORMAL_CONNECTION)
                        .addConnectedEntity(RG_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
        final TestGraphEntity.Builder storageTier =
                TestGraphEntity.newBuilder(6L, ApiEntityType.STORAGE_TIER)
                        .addConnectedEntity(RG_ID, ConnectionType.AGGREGATED_BY_CONNECTION);
        final Long2ObjectMap<Builder> cloudTopologyMap = new Long2ObjectOpenHashMap<>();
        cloudTopologyMap.put(RG_ID, region);
        cloudTopologyMap.put(AZ_ID, zone);
        cloudTopologyMap.put(DB_ID, db);
        cloudTopologyMap.put(VM_ID, vm);
        cloudTopologyMap.put(CT_ID, computeTier);
        cloudTopologyMap.put(ST_ID, storageTier);
        return TestGraphEntity.newGraph(cloudTopologyMap);
    }
}
