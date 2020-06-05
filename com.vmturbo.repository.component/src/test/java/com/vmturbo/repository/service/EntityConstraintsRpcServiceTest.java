package com.vmturbo.repository.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javafx.util.Pair;

import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraint;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsRequest;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacements;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsRequest;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraintsServiceGrpc;
import com.vmturbo.common.protobuf.repository.EntityConstraintsServiceGrpc.EntityConstraintsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * Tests the Entity constraints service.
 */
public class EntityConstraintsRpcServiceTest {

    private static final long VM1 = 1;
    // OID and entity type
    private static final Pair<Long, Integer> PM1 = new Pair(10L, EntityType.PHYSICAL_MACHINE_VALUE);
    private static final Pair<Long, Integer> PM2 = new Pair(11L, EntityType.PHYSICAL_MACHINE_VALUE);
    private static final Pair<Long, Integer> PM3 = new Pair(12L, EntityType.PHYSICAL_MACHINE_VALUE);
    private static final Pair<Long, Integer> ST1 = new Pair(20L, EntityType.STORAGE_VALUE);
    private static final Pair<Long, Integer> ST2 = new Pair(21L, EntityType.STORAGE_VALUE);
    private static final Pair<Long, Integer> ST3 = new Pair(22L, EntityType.STORAGE_VALUE);
    private final LiveTopologyStore liveTopologyStore = new LiveTopologyStore(new GlobalSupplyChainCalculator());
    private final PartialEntityConverter partialEntityConverter = new PartialEntityConverter();
    EntityConstraintsRpcService constraintsService = new EntityConstraintsRpcService(
        liveTopologyStore, partialEntityConverter, new ConstraintsCalculator());
    /**
     * Test server.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(constraintsService);
    private EntityConstraintsServiceBlockingStub clientStub;

    /**
     * Set up a topology with:
     * VM1 which buys CPU, license access, datacenter and segmentation from PM1, and storage cluster and dspm from St1.
     * PM1 which sells CPU, license access, datacenter and segmentation.
     * PM2 which sells CPU, license access, datacenter.
     * PM3 which sells CPU, datacenter.
     * ST1 which sells storage cluster and dspm.
     * ST2 which sells storage cluster and dspm (but the keys are not the keys the VM needs).
     * ST3 which sells storage cluster and dspm.
     */
    @Before
    public void setUp() {
        final SourceRealtimeTopologyBuilder topologyBuilder =
            liveTopologyStore.newRealtimeTopology(TopologyInfo.getDefaultInstance());
        topologyBuilder.addEntities(ImmutableList.of(
            createConsumer(EntityType.VIRTUAL_MACHINE_VALUE, VM1,
                ImmutableMap.of(
                    PM1, ImmutableSet.of(
                        createCommodityBought(CommodityDTO.CommodityType.CPU_VALUE, StringUtils.EMPTY),
                        createCommodityBought(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE, "Linux"),
                        createCommodityBought(CommodityDTO.CommodityType.DATACENTER_VALUE, "DC1"),
                        createCommodityBought(CommodityDTO.CommodityType.SEGMENTATION_VALUE, "policy1")),
                    ST1, ImmutableSet.of(
                        createCommodityBought(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE, "free"),
                        createCommodityBought(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE, "PhysicalMachine::PM1"))), "VM1"),
            createProvider(EntityType.PHYSICAL_MACHINE_VALUE, PM1.getKey(), ImmutableList.of(
                createCommoditySold(CommodityDTO.CommodityType.CPU_VALUE, StringUtils.EMPTY),
                createCommoditySold(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE, "Linux"),
                createCommoditySold(CommodityDTO.CommodityType.DATACENTER_VALUE, "DC1"),
                createCommoditySold(CommodityDTO.CommodityType.SEGMENTATION_VALUE, "policy1")), "PM1"),
            createProvider(EntityType.PHYSICAL_MACHINE_VALUE, PM2.getKey(), ImmutableList.of(
                createCommoditySold(CommodityDTO.CommodityType.CPU_VALUE, StringUtils.EMPTY),
                createCommoditySold(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE, "Linux"),
                createCommoditySold(CommodityDTO.CommodityType.DATACENTER_VALUE, "DC1")), "PM2"),
            createProvider(EntityType.PHYSICAL_MACHINE_VALUE, PM3.getKey(), ImmutableList.of(
                createCommoditySold(CommodityDTO.CommodityType.CPU_VALUE, StringUtils.EMPTY),
                createCommoditySold(CommodityDTO.CommodityType.DATACENTER_VALUE, "DC1")), "PM3"),
            createProvider(EntityType.STORAGE_VALUE, ST1.getKey(), ImmutableList.of(
                createCommoditySold(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE, "free"),
                createCommoditySold(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE, "PhysicalMachine::PM1")), "ST1"),
            createProvider(EntityType.STORAGE_VALUE, ST2.getKey(), ImmutableList.of(
                createCommoditySold(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE, "free"),
                createCommoditySold(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE, "PhysicalMachine::PM2")), "ST2"),
            createProvider(EntityType.STORAGE_VALUE, ST3.getKey(), ImmutableList.of(
                createCommoditySold(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE, "free"),
                createCommoditySold(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE, "PhysicalMachine::PM1")), "ST3")
            ));
        topologyBuilder.finish();
        clientStub = EntityConstraintsServiceGrpc.newBlockingStub(testServer.getChannel());
    }

    /**
     * Get the constraints for VM1.
     * Ensure we have 2 high level constraints, one for PM and one for storage
     * Verify the number of potential placements for each.
     */
    @Test
    public void testGetConstraints() {
        EntityConstraintsResponse response = clientStub.getConstraints(
            EntityConstraintsRequest.newBuilder().setOid(VM1).build());
        Map<Integer, EntityConstraint> constraints = response
            .getEntityConstraintList().stream().collect(Collectors.toMap(c -> c.getEntityType(), Function.identity()));
        // Ensure we have 2 high level constraints, one for PM and one for storage
        assertEquals(2, constraints.size());
        assertTrue(constraints.containsKey(EntityType.PHYSICAL_MACHINE_VALUE));
        assertTrue(constraints.containsKey(EntityType.STORAGE_VALUE));
        // Check that the PM constraint has 1 overall potential placement
        // And we have potentialPlacement entries for license, dc and segmentation, but not for CPU.
        // Verify the number potential placements for each.
        EntityConstraint computeConstraint = constraints.get(EntityType.PHYSICAL_MACHINE_VALUE);
        assertEquals(PM1.getKey(), (Long)computeConstraint.getCurrentPlacement().getOid());
        assertEquals(1, computeConstraint.getNumPotentialPlacements());
        Map<Integer, PotentialPlacements> computePotentialPlacements =
            computeConstraint.getPotentialPlacementsList().stream().collect(
                Collectors.toMap(x -> x.getCommodityType().getType(), Function.identity()));
        assertEquals(3, computePotentialPlacements.size());
        assertEquals(2, computePotentialPlacements.get(
            CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE).getNumPotentialPlacements());
        assertEquals(3, computePotentialPlacements.get(
            CommodityDTO.CommodityType.DATACENTER_VALUE).getNumPotentialPlacements());
        assertEquals(1, computePotentialPlacements.get(
            CommodityDTO.CommodityType.SEGMENTATION_VALUE).getNumPotentialPlacements());
        assertFalse(computePotentialPlacements.containsKey(CommodityDTO.CommodityType.CPU));
        // Check that the Storage constraint has 2 overall potential placement.
        // And we have potentialPlacement entries for storage cluster and dspm.
        // Verify the number potential placements for each.
        EntityConstraint storageConstraint = constraints.get(EntityType.STORAGE_VALUE);
        assertEquals(ST1.getKey(), (Long)storageConstraint.getCurrentPlacement().getOid());
        assertEquals(2, storageConstraint.getNumPotentialPlacements());
        Map<Integer, PotentialPlacements> storagePotentialPlacements =
            storageConstraint.getPotentialPlacementsList().stream().collect(
                Collectors.toMap(x -> x.getCommodityType().getType(), Function.identity()));
        assertEquals(2, storagePotentialPlacements.size());
        assertEquals(3, storagePotentialPlacements.get(
            CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE).getNumPotentialPlacements());
        assertEquals(2, storagePotentialPlacements.get(
            CommodityDTO.CommodityType.DSPM_ACCESS_VALUE).getNumPotentialPlacements());
    }

    /**
     * Test getting the potential placements.
     * Request contains license access and dc. These commodities can be satisfied by PM1 and PM2.
     * Ensure that we get back PM1 and PM2 in the response.
     */
    @Test
    public void testGetPotentialPlacements() {
        PotentialPlacementsRequest request = PotentialPlacementsRequest.newBuilder()
            .addPotentialEntityTypes(EntityType.PHYSICAL_MACHINE_VALUE)
            .addAllCommodityType(ImmutableList.of(
                createCommodityType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE, "Linux").build(),
                createCommodityType(CommodityDTO.CommodityType.DATACENTER_VALUE, "DC1").build()))
            .build();
        PotentialPlacementsResponse response = clientStub.getPotentialPlacements(request);

        assertEquals(2, response.getNumPotentialPlacements());
        assertEquals(ImmutableSet.of(PM1.getKey(), PM2.getKey()), response
            .getEntitiesList().stream().map(e -> e.getApi().getOid()).collect(Collectors.toSet()));
    }

    private TopologyEntityDTO createConsumer(int entityType, long oid, Map<Pair<Long, Integer>,
        Set<CommodityBoughtDTO>> commBoughtFromProviders, String displayName) {
        Set<CommoditiesBoughtFromProvider> legs = new HashSet<>();
        commBoughtFromProviders.forEach((provider, commsBought) -> {
            CommoditiesBoughtFromProvider.Builder leg =
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(provider.getKey())
                    .setProviderEntityType(provider.getValue());
            commsBought.forEach(leg::addCommodityBought);
            legs.add(leg.build());
        });
        return TopologyEntityDTO.newBuilder()
            .setEntityType(entityType)
            .setOid(oid)
            .addAllCommoditiesBoughtFromProviders(legs)
            .setDisplayName(displayName)
            .build();
    }

    private TopologyEntityDTO createProvider(int entityType, long oid,
                                             List<CommoditySoldDTO> commoditiesSold,
                                             String displayName) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(entityType)
            .setOid(oid)
            .addAllCommoditySoldList(commoditiesSold)
            .setDisplayName(displayName)
            .build();
    }

    private CommodityBoughtDTO createCommodityBought(int commodityType, String key) {
        return CommodityBoughtDTO.newBuilder().setCommodityType(createCommodityType(commodityType, key)).build();
    }

    private CommoditySoldDTO createCommoditySold(int commodityType, String key) {
        return CommoditySoldDTO.newBuilder().setCommodityType(createCommodityType(commodityType, key)).build();
    }

    private CommodityType.Builder createCommodityType(int commodityType, String key) {
        CommodityType.Builder result = CommodityType.newBuilder().setType(commodityType);
        if (!StringUtils.EMPTY.equals(key)) {
            result.setKey(key);
        }
        return result;
    }
}