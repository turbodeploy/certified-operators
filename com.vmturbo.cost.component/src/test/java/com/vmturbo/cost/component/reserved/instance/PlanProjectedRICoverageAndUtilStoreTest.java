package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityBatch;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceCoverageRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceUtilizationRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class PlanProjectedRICoverageAndUtilStoreTest {
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private DSLContext dsl;
    private Flyway flyway;

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore = mock(ReservedInstanceBoughtStore.class);
    private ReservedInstanceSpecStore reservedInstanceSpecStore = mock(ReservedInstanceSpecStore.class);
    private RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private GrpcTestServer testServer = GrpcTestServer.newServer(repositoryService);
    private PlanProjectedRICoverageAndUtilStore store;
    private static final EntityReservedInstanceCoverage ENTITY_RI_COVERAGE =
            EntityReservedInstanceCoverage.newBuilder()
                    .setEntityId(1L)
                    .putCouponsCoveredByRi(10L, 100.0)
                    .build();
    private final TopologyInfo topoInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(20l)
            .setTopologyId(0l)
            .build();
    private final int chunkSize = 10;

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        testServer.start();
        // set time out on topology available or failure for 1 seconds
        store = Mockito.spy(new PlanProjectedRICoverageAndUtilStore(dsl, 1, RepositoryServiceGrpc
                .newBlockingStub(testServer.getChannel()), reservedInstanceBoughtStore,
                reservedInstanceSpecStore, chunkSize));
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testUpdateProjectedEntityToRIMappingTableForPlan() {
        store.updateProjectedEntityToRIMappingTableForPlan(topoInfo, Arrays.asList(ENTITY_RI_COVERAGE));
        final List<PlanProjectedEntityToReservedInstanceMappingRecord> records = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING).fetch();
        assertEquals(1, records.size());
        PlanProjectedEntityToReservedInstanceMappingRecord rcd = records.get(0);
        assertTrue(rcd.getEntityId() == 1l);
        assertTrue(rcd.getPlanId() == 20l);
        assertTrue(rcd.getReservedInstanceId() == 10l);
        assertTrue(rcd.getUsedCoupons() == 100);
    }

    @Test
    public void testUpdateProjectedRIUtilTableForPlan() {
        List<ReservedInstanceBought> riBought = new ArrayList<>();
        riBought.add(ReservedInstanceBought.newBuilder().setId(10l)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(701l)
                        .setAvailabilityZoneId(1000l)
                        .setBusinessAccountId(2l)
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons
                                .newBuilder().setNumberOfCoupons(100)))
                .build());
        riBought.add(ReservedInstanceBought.newBuilder().setId(5l)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(702l)
                        .setAvailabilityZoneId(2000l)
                        .setBusinessAccountId(2l)
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons
                                .newBuilder().setNumberOfCoupons(200)))
                .build());
        when(reservedInstanceBoughtStore
             .getReservedInstanceBoughtByFilter(any())).thenReturn(riBought);
        Set<Long> riSpecId = new HashSet<Long>();
        riSpecId.add(701l);
        riSpecId.add(702l);
        List<ReservedInstanceSpec> specs = new ArrayList<>();
        specs.add(ReservedInstanceSpec.newBuilder().setId(701l)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setRegionId(3000l))
                .build());
        specs.add(ReservedInstanceSpec.newBuilder().setId(702l)
                  .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setRegionId(4000l))
                  .build());
        when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any())).thenReturn(specs);

        store.updateProjectedRIUtilTableForPlan(topoInfo, Arrays.asList(ENTITY_RI_COVERAGE));
        final List<PlanProjectedReservedInstanceUtilizationRecord> records = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION).fetch();
        assertEquals(1, records.size());
        PlanProjectedReservedInstanceUtilizationRecord rcd = records.get(0);
        assertTrue(rcd.getId() == 10l);
        assertTrue(rcd.getPlanId() == 20l);
        assertTrue(rcd.getAvailabilityZoneId() == 1000l);
        assertTrue(rcd.getBusinessAccountId() == 2l);
        assertTrue(rcd.getTotalCoupons() == 100);
        assertTrue(rcd.getUsedCoupons() == 100);
    }

    @Test
    public void testUpdateProjectedRICoverageTableForPlan() {
        Map<Long, TopologyEntityDTO> entityMap = getEntityMap();
        long projectedTopoId = 12300l;
        store.onProjectedTopologyAvailable(projectedTopoId, topoInfo.getTopologyContextId());
        // assuming ri oid is 5 and it is used by vm with oid 101l
        @SuppressWarnings("unchecked")
        Map<Long, Double> riUsage = new HashMap<>();
        riUsage.put(5l, 0.2);
        List<EntityReservedInstanceCoverage> entityRICoverage = Arrays
                        .asList(EntityReservedInstanceCoverage
                        .newBuilder()
                        .setEntityId(101l)
                        .putAllCouponsCoveredByRi(riUsage)
                        .build());
        when(repositoryService.retrieveTopologyEntities(any()))
                .thenReturn(Arrays.asList(EntityBatch.newBuilder()
                        .addAllEntities(entityMap.values()).build()));
        store.updateProjectedRICoverageTableForPlan(projectedTopoId, topoInfo, entityRICoverage);
        final List<PlanProjectedReservedInstanceCoverageRecord> records = dsl
                        .selectFrom(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE).fetch();
        assertEquals(1, records.size());
        PlanProjectedReservedInstanceCoverageRecord rcd = records.get(0);
        assertTrue(rcd.getEntityId() == 101l);
        assertTrue(rcd.getPlanId() == 20l);
        assertTrue(rcd.getRegionId() == 2000l);
        assertTrue(rcd.getAvailabilityZoneId() == 1000l);
        dsl.delete(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE);
    }

    private Map<Long, TopologyEntityDTO> getEntityMap() {
        Map<Long, TopologyEntityDTO> entityMap = new HashMap();
     // build up a topology in which az is owned by region,
        // vm connectedTo az and consumes computeTier,
        // computeTier connectedTo region,
        // ba connectedTo vm
        TopologyEntityDTO az = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setOid(1000l)
                        .build();
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(2000l)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(1000l)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();
        TopologyEntityDTO ba = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(3000l)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(101l)
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                .build();
        TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setOid(4000l)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(2000l)
                                .setConnectedEntityType(EntityType.REGION_VALUE))
                        .build();
        TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(101l)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                         .setProviderId(4000l)
                         .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(1000l)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                .build();
        entityMap.put(1000l, az);
        entityMap.put(2000l, region);
        entityMap.put(3000l, ba);
        entityMap.put(4000l, computeTier);
        entityMap.put(101l, vm);
        return entityMap;
    }

    @Test
    public void testOnProjectedTopologyAvailableAndOnProjectedTopologyFailure() {
        long projectedTopoId1 = 100011l;
        long projectedTopoId2 = 100012l;
        long contextId = 120001l;
        @SuppressWarnings("unchecked")
        Map<Long, Double> riUsage = new HashMap<>();
        riUsage.put(5l, 0.2);
        List<EntityReservedInstanceCoverage> entityRICoverage = Arrays
                        .asList(EntityReservedInstanceCoverage
                        .newBuilder()
                        .setEntityId(101l)
                        .putAllCouponsCoveredByRi(riUsage)
                        .build());
        when(repositoryService.retrieveTopologyEntities(any()))
            .thenReturn(Arrays.asList(EntityBatch.newBuilder()
                .addAllEntities(getEntityMap().values()).build()));
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
        assertTrue(rcd.getEntityId() == 101l);
        assertTrue(rcd.getPlanId() == 20l);
        assertTrue(rcd.getRegionId() == 2000l);
        assertTrue(rcd.getAvailabilityZoneId() == 1000l);
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
}
