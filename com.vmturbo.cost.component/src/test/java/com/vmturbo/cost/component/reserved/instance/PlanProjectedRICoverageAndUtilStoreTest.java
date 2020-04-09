package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceCoverageRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceUtilizationRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class PlanProjectedRICoverageAndUtilStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final long PLAN_ID = 20L;
    private static final double DELTA = 0.01;

    private DSLContext dsl = dbConfig.getDslContext();

    private ReservedInstanceSpecStore reservedInstanceSpecStore = mock(ReservedInstanceSpecStore.class);
    private RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private RepositoryClient repositoryClient = mock(RepositoryClient.class);
    private SupplyChainServiceBlockingStub  supplyChainService;
    private PlanReservedInstanceServiceBlockingStub planReservedInstanceService;
    private final Long realtimeTopologyContextId = 777777L;

    private PlanProjectedRICoverageAndUtilStore store;

    private static final EntityReservedInstanceCoverage ENTITY_RI_COVERAGE =
            EntityReservedInstanceCoverage.newBuilder()
                    .setEntityId(1L)
                    .putCouponsCoveredByRi(10L, 100.0)
                    .setEntityCouponCapacity(10)
                    .build();
    private final TopologyInfo topoInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(PLAN_ID)
            .setTopologyId(0L)
            .build();
    private final int chunkSize = 10;

    private PlanReservedInstanceStore planReservedInstanceStore =
            Mockito.mock(PlanReservedInstanceStore.class);
    private BuyReservedInstanceStore buyReservedInstanceStore = mock(BuyReservedInstanceStore.class);

    private PlanReservedInstanceRpcService planRiService = new PlanReservedInstanceRpcService(
            planReservedInstanceStore, buyReservedInstanceStore);

    /**
     * Test gRPC server for mocking gRPC dependencies.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(repositoryService);

    /**
     * gRPC server for plan service.
     */
    @Rule
    public GrpcTestServer planGrpcServer = GrpcTestServer.newServer(planRiService);


    @Before
    public void setup() throws Exception {
        supplyChainService = SupplyChainServiceGrpc.newBlockingStub(testServer.getChannel());
        planReservedInstanceService = PlanReservedInstanceServiceGrpc.newBlockingStub(planGrpcServer.getChannel());
        // set time out on topology available or failure for 1 seconds
        store = Mockito.spy(new PlanProjectedRICoverageAndUtilStore(dsl, 1, RepositoryServiceGrpc
              .newBlockingStub(testServer.getChannel()), repositoryClient,
                planReservedInstanceService, reservedInstanceSpecStore, supplyChainService, chunkSize,
               realtimeTopologyContextId));
    }

    @Test
    public void testUpdateProjectedEntityToRIMappingTableForPlan() {
        store.updateProjectedEntityToRIMappingTableForPlan(topoInfo, Arrays.asList(ENTITY_RI_COVERAGE));
        final List<PlanProjectedEntityToReservedInstanceMappingRecord> records = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING).fetch();
        assertEquals(1, records.size());
        PlanProjectedEntityToReservedInstanceMappingRecord rcd = records.get(0);
        assertEquals(1L, rcd.getEntityId(), DELTA);
        assertEquals(PLAN_ID, rcd.getPlanId(), DELTA);
        assertEquals(10L, rcd.getReservedInstanceId(), DELTA);
        assertEquals(100, rcd.getUsedCoupons(), DELTA);
    }

    @Test
    public void testUpdateProjectedRIUtilTableForPlan() {
        mockPlanRIUtilizationTables();
        final List<PlanProjectedReservedInstanceUtilizationRecord> records = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION).fetch();
        assertEquals(2, records.size());
        PlanProjectedReservedInstanceUtilizationRecord rcd = records.get(0);
        assertEquals(10L, rcd.getId(), DELTA);
        assertEquals(PLAN_ID, rcd.getPlanId(), DELTA);
        assertEquals(1000L, rcd.getAvailabilityZoneId(), DELTA);
        assertEquals(2L, rcd.getBusinessAccountId(), DELTA);
        assertEquals(100, rcd.getTotalCoupons(), DELTA);
        assertEquals(100, rcd.getUsedCoupons(), DELTA);

        rcd = records.get(1);
        assertEquals(11L, rcd.getId(), DELTA);
        assertEquals(PLAN_ID, rcd.getPlanId(), DELTA);
        assertEquals(0L, rcd.getAvailabilityZoneId(), DELTA);
        assertEquals(2L, rcd.getBusinessAccountId(), DELTA);
        assertEquals(200, rcd.getTotalCoupons(), DELTA);
        assertEquals(0, rcd.getUsedCoupons(), DELTA);
    }

    /**
     * Updates dummy RIs bought to simulate those selected by user in OCP RI inventory
     * widget. Coupon capacity of these RIs is used to show total projected capacity value.
     */
    private void updateReservedInstanceBought() {
        List<ReservedInstanceBought> selectedRis = new ArrayList<>();
        selectedRis.add(ReservedInstanceBought.newBuilder()
                .setId(10)
                .setReservedInstanceBoughtInfo(
                        ReservedInstanceBoughtInfo.newBuilder()
                                .setReservedInstanceSpec(701L)
                                .setBusinessAccountId(2)
                                .setAvailabilityZoneId(1000)
                                .setNumBought(1)
                                .setDisplayName("m5.large")
                                .setReservedInstanceBoughtCoupons(
                                        ReservedInstanceBoughtCoupons.newBuilder()
                                                .setNumberOfCoupons(100)
                                )
                ).build());
        selectedRis.add(ReservedInstanceBought.newBuilder()
                .setId(11)
                .setReservedInstanceBoughtInfo(
                        ReservedInstanceBoughtInfo.newBuilder()
                                .setReservedInstanceSpec(702L)
                                .setBusinessAccountId(2)
                                .setAvailabilityZoneId(0)
                                .setNumBought(1)
                                .setDisplayName("t5.large")
                                .setReservedInstanceBoughtCoupons(
                                        ReservedInstanceBoughtCoupons.newBuilder()
                                                .setNumberOfCoupons(200)
                                )
                ).build());

        final UploadRIDataRequest uploadRequest =
                UploadRIDataRequest
                        .newBuilder()
                        .setTopologyId(PLAN_ID)
                        .addAllReservedInstanceBought(selectedRis)
                        .build();
        planReservedInstanceService.insertPlanReservedInstanceBought(uploadRequest);
        when(planReservedInstanceStore.getReservedInstanceBoughtByPlanId(PLAN_ID))
                .thenReturn(selectedRis);
    }

    private void mockPlanRIUtilizationTables() {
        updateReservedInstanceBought();
        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceSpec.newBuilder().setId(701L)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setRegionId(3000L))
                .build());
        specs.add(ReservedInstanceSpec.newBuilder().setId(702L)
                  .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setRegionId(4000L))
                  .build());
        when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any())).thenReturn(specs);

        store.updateProjectedRIUtilTableForPlan(topoInfo, Arrays.asList(ENTITY_RI_COVERAGE), new ArrayList<>());
    }

    @Test
    public void testUpdateProjectedRICoverageTableForPlan() {
        long projectedTopoId = 12300L;
        mockPlanProjectedRICoverageTable(projectedTopoId, false);
        final List<PlanProjectedReservedInstanceCoverageRecord> records = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE).fetch();
        assertEquals(1, records.size());
        PlanProjectedReservedInstanceCoverageRecord rcd = records.get(0);
        assertEquals(101L, rcd.getEntityId(), DELTA);
        assertEquals(PLAN_ID, rcd.getPlanId(), DELTA);
        assertEquals(2000L, rcd.getRegionId(), DELTA);
        assertEquals(1000L, rcd.getAvailabilityZoneId(), DELTA);
        dsl.delete(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE);
    }

    private void mockPlanProjectedRICoverageTable(long projectedTopoId, boolean noZones) {
        final Map<Long, TopologyEntityDTO> entityMap = getEntityMap(noZones);
        store.onProjectedTopologyAvailable(projectedTopoId, topoInfo.getTopologyContextId());
        // assuming ri oid is 5 and it is used by vm with oid 101L
        final Map<Long, Double> riUsage = new HashMap<>();
        riUsage.put(5L, 0.2);
        final List<EntityReservedInstanceCoverage> entityRICoverage = Arrays.asList(EntityReservedInstanceCoverage
            .newBuilder()
            .setEntityId(101L)
            .putAllCouponsCoveredByRi(riUsage)
            .setEntityCouponCapacity(5)
            .build());
        when(repositoryService.retrieveTopologyEntities(any()))
            .thenReturn(Arrays.asList(PartialEntityBatch.newBuilder()
                .addAllEntities(entityMap.values()
                    .stream()
                    .map(e -> PartialEntity.newBuilder()
                        .setFullEntity(e)
                        .build())
                    .collect(Collectors.toList()))
                .build()));
        store.updateProjectedRICoverageTableForPlan(projectedTopoId, topoInfo, entityRICoverage);
    }

    private static Map<Long, TopologyEntityDTO> getEntityMap(boolean noZones) {
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        // build up a topology in which az is owned by region,
        // vm connectedTo az and consumes computeTier,
        // computeTier connectedTo region,
        // ba connectedTo vm
        TopologyEntityDTO az = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setOid(1000L)
                        .build();
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(2000L)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(1000L)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();
        TopologyEntityDTO ba = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(3000L)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(101L)
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build();
        TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setOid(4000L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(2000L)
                                .setConnectedEntityType(EntityType.REGION_VALUE))
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setComputeTier(ComputeTierInfo.newBuilder().setNumCoupons(10)))
                        .build();

        TopologyEntityDTO.Builder vmBuilder = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(101L)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                         .setProviderId(4000L)
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE));
        if (noZones) {
            vmBuilder.addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(2000L)
                    .setConnectedEntityType(EntityType.REGION_VALUE));
        } else {
            vmBuilder.addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(1000L)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE));
        }
        TopologyEntityDTO vm = vmBuilder.build();

        entityMap.put(1000L, az);
        entityMap.put(2000L, region);
        entityMap.put(3000L, ba);
        entityMap.put(4000L, computeTier);
        entityMap.put(101L, vm);
        return entityMap;
    }

    @Test
    public void testOnProjectedTopologyAvailableAndOnProjectedTopologyFailure() {
        long projectedTopoId1 = 100011L;
        long projectedTopoId2 = 100012L;
        long contextId = 120001L;
        @SuppressWarnings("unchecked")
        Map<Long, Double> riUsage = new HashMap<>();
        riUsage.put(5L, 0.2);
        List<EntityReservedInstanceCoverage> entityRICoverage = Arrays
                        .asList(EntityReservedInstanceCoverage
                        .newBuilder()
                        .setEntityId(101L)
                        .putAllCouponsCoveredByRi(riUsage)
                        .setEntityCouponCapacity(5)
                        .build());
        when(repositoryService.retrieveTopologyEntities(any()))
            .thenReturn(Arrays.asList(PartialEntityBatch.newBuilder()
                    .addAllEntities(getEntityMap(false).values().stream()
                    .map(e -> PartialEntity.newBuilder()
                        .setFullEntity(e)
                        .build())
                    .collect(Collectors.toList()))
                .build()));
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                //trigger RI coverage update
                store.updateProjectedRICoverageTableForPlan(projectedTopoId1, topoInfo, entityRICoverage);
            }
        });
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                //when projected topology is available
                store.onProjectedTopologyAvailable(projectedTopoId1, contextId);
            }
        });
        Thread thread3 = new Thread(new Runnable() {
            @Override
            public void run() {
                store.updateProjectedRICoverageTableForPlan(projectedTopoId2, topoInfo, entityRICoverage);
            }
        });
        Thread thread4 = new Thread(new Runnable() {
            @Override
            public void run() {
                //when projected topology is failed
                store.onProjectedTopologyFailure(projectedTopoId2, contextId, "failed");
            }
        });
        thread1.start();
        thread2.start();
        try {
            Thread.sleep(3000);//wait 3 seconds to check the result
        } catch (Exception e) {
            e.printStackTrace();
        }
        final List<PlanProjectedReservedInstanceCoverageRecord> records = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE).fetch();
        assertEquals(1, records.size());// 1 record is added
        PlanProjectedReservedInstanceCoverageRecord rcd = records.get(0);
        assertEquals(101L, rcd.getEntityId(), DELTA);
        assertEquals(PLAN_ID, rcd.getPlanId(), DELTA);
        assertEquals(2000L, rcd.getRegionId(), DELTA);
        assertEquals(1000L, rcd.getAvailabilityZoneId(), DELTA);
        thread3.start();
        thread4.start();
        try {
            Thread.sleep(3000);//wait 3 seconds to check the result
        } catch (Exception e) {
            e.printStackTrace();
        }
        final List<PlanProjectedReservedInstanceCoverageRecord> newRecords = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE).fetch();
        assertEquals(1, newRecords.size()); //still has only 1 record
    }

    /**
     * Test getting of RI utilization stats from DB.
     */
    @Test
    public void testGetReservedInstanceUtilizationStatsRecords() {
        mockPlanRIUtilizationTables();
        final List<ReservedInstanceStatsRecord> statsRecords =
                store.getPlanReservedInstanceUtilizationStatsRecords(PLAN_ID, Collections.emptyList());
        assertEquals(1, statsRecords.size());
        final ReservedInstanceStatsRecord record = statsRecords.get(0);
        // Coupon capacities are 100 and 200, so average is 150.
        assertEquals(150, record.getCapacity().getAvg(), DELTA);
        // Used coupons is 100 out of 2 coupons, so average is 50.
        assertEquals(50, record.getValues().getAvg(), DELTA);
    }

    /**
     * Test getting of RI coverage stats from DB.
     */
    @Test
    public void testGetReservedInstanceCoverageStatsRecords() {
        mockPlanRIUtilizationTables();
        mockPlanProjectedRICoverageTable(PLAN_ID, false);
        final List<ReservedInstanceStatsRecord> statsRecords =
                store.getPlanReservedInstanceCoverageStatsRecords(PLAN_ID, Collections.emptyList());
        assertEquals(1, statsRecords.size());
        final ReservedInstanceStatsRecord record = statsRecords.get(0);
        assertEquals(5, record.getCapacity().getAvg(), DELTA);
        assertEquals(0.2, record.getValues().getAvg(), DELTA);
    }

    /**
     * Test getting of RI coverage stats from DB when the entity is connected directly to the Region, not to the Zone.
     */
    @Test
    public void testGetReservedInstanceCoverageStatsRecordsWithoutZones() {
        mockPlanRIUtilizationTables();
        mockPlanProjectedRICoverageTable(PLAN_ID, true);
        final List<ReservedInstanceStatsRecord> statsRecords =
                store.getPlanReservedInstanceCoverageStatsRecords(PLAN_ID, Collections.emptyList());
        assertEquals(1, statsRecords.size());
        final ReservedInstanceStatsRecord record = statsRecords.get(0);
        assertEquals(5, record.getCapacity().getAvg(), DELTA);
        assertEquals(0.2, record.getValues().getAvg(), DELTA);
    }

    /**
     * Tests getting accumulated RI coverage map that merges RI coverages for each entity.
     */
    @Test
    public void testGetAccumulatedRICoverageMap() {
        EntityReservedInstanceCoverage riCoverage1 = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(1L)
                .putCouponsCoveredByRi(1L, 0.5)
                .build();
        EntityReservedInstanceCoverage riCoverage2 = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(1L)
                .putCouponsCoveredByRi(1L, 0.5)
                .build();
        EntityReservedInstanceCoverage riCoverage3 = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(2L)
                .putCouponsCoveredByRi(1L, 2.0)
                .build();
        EntityReservedInstanceCoverage riCoverage4 = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(4L)
                .putCouponsCoveredByRi(1L, 1.0)
                .build();
        EntityReservedInstanceCoverage riCoverage5 = EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(4L)
                .putCouponsCoveredByRi(1L, 1.0)
                .putCouponsCoveredByRi(2L, 2.0)
                .build();
        List<EntityReservedInstanceCoverage> entityRICoverage = Arrays.asList(riCoverage1,
                riCoverage2, riCoverage3, riCoverage4, riCoverage5);
        Map<Long, Double> accumulatedRICoverage = store.getAggregatedEntityRICoverage(entityRICoverage);
        // check coverage for entity ID 1
        assertEquals(new Double(1.0), accumulatedRICoverage.get(1L));
        // Check coverage for entity ID 2
        assertEquals(new Double(2.0), accumulatedRICoverage.get(2L));
        // Check coverage for entity ID 4
        assertEquals(new Double(4.0), accumulatedRICoverage.get(4L));
    }

}
