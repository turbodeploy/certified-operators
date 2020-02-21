package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit tests for {@link ProjectedRICoverageAndUtilStore}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class ProjectedRICoverageAndUtilStoreTest {

    private final Long realtimeTopologyContextId = 777777L;
    private RepositoryClient repositoryClient = mock(RepositoryClient.class);
    private SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());
    private GrpcTestServer testServer = GrpcTestServer.newServer(supplyChainServiceMole);
    private ProjectedRICoverageAndUtilStore store;
    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);
    private final BuyReservedInstanceStore buyReservedInstanceStore =
            mock(BuyReservedInstanceStore.class);
    private final Clock clock = Clock.systemUTC();

    private static final long VM_1_ID = 1L;
    private static final long VM_2_ID = 2L;
    private static final long REGION_1_ID = 3L;
    private static final long ACCOUNT_ID = 11111L;

    private static final EntityReservedInstanceCoverage ENTITY_RI_COVERAGE =
        EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(VM_1_ID)
                .setEntityCouponCapacity(200)
                .putCouponsCoveredByRi(10L, 100.0)
                .putCouponsCoveredByBuyRi(11L, 100.0)
                .build();
    private static final EntityReservedInstanceCoverage SECOND_RI_COVERAGE =
                    EntityReservedInstanceCoverage.newBuilder()
                            .setEntityId(VM_2_ID)
                            .setEntityCouponCapacity(100)
                            .putCouponsCoveredByRi(4L, 50.0)
                            .putCouponsCoveredByBuyRi(7L, 25.0).build();
    private final TopologyInfo topoInfo =
                    TopologyInfo.newBuilder().setTopologyContextId(realtimeTopologyContextId)
                                    .setTopologyId(0L).build();
    // The code should ignore the REGION Set
    private final Map<EntityType, Set<Long>> scopedOids =
                    ImmutableMap.of(EntityType.VIRTUAL_MACHINE, ImmutableSet.of(VM_2_ID),
                                    EntityType.REGION, ImmutableSet.of(REGION_1_ID));

    /**
     * Create an empty instance of ProjectedRICoverageAndUtilStore for each test.
     *
     * @throws Exception
     *     when something goes wrong
     */
    @Before
    public void setup() throws Exception {
        testServer.start();
        final SupplyChainServiceBlockingStub supplyChainService = SupplyChainServiceGrpc.newBlockingStub(testServer.getChannel());
        store = Mockito.spy(new ProjectedRICoverageAndUtilStore(
                repositoryClient,
                supplyChainService,
                reservedInstanceBoughtStore,
                buyReservedInstanceStore,
                clock));
    }

    /**
     * Verify that we get back what we put in the map if we get all projected entities' coverage.
     */
    @Test
    public void testUpdateAndGet() {
        store.updateProjectedRICoverage(topoInfo, Collections.singletonList(ENTITY_RI_COVERAGE));
        final Map<Long, EntityReservedInstanceCoverage> retCostMap = store.getAllProjectedEntitiesRICoverages();
        assertThat(retCostMap, is(ImmutableMap.of(ENTITY_RI_COVERAGE.getEntityId(), ENTITY_RI_COVERAGE)));
    }

    /**
     * When the ProjectedRICoverage contains entries for both VM_1_ID and VM_2_ID and we scope to
     * VM_2_ID only verify we get back VM_2's coverage only.
     */
    @Test
    public void testUpdateAndScopedGet() {
        // Store a map with coverage for both VM_1_ID and VM_2_ID
        store.updateProjectedRICoverage(topoInfo,
                        Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        // Create a filter, the when below means we ignore the contents
        ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_1_ID)
                        .build())
                .build();
        // Scope to VM_2_ID only
        when(repositoryClient.getEntitiesByTypePerScope(any(), any()))
                .thenReturn(Stream.of(scopedOids));
        // Get the scoped Map
        final Map<Long, EntityReservedInstanceCoverage> retCostMap =
                        store.getScopedProjectedEntitiesRICoverages(filter);
        // Map should only contain VM_2_ID's data
        assertThat(retCostMap, is(ImmutableMap.of(SECOND_RI_COVERAGE.getEntityId(), SECOND_RI_COVERAGE)));
    }

    /**
     * Verify that at creation of an instance the map is empty.
     */
    @Test
    public void testGetEmpty() {
        assertThat(store.getAllProjectedEntitiesRICoverages(),
                is(Collections.emptyMap()));
    }

    @Test
    public void testGetReservedInstanceCoverageStats() {

        // Store a map with coverage for both VM_1_ID and VM_2_ID
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        // Create a filter, the when below means we ignore the contents
        ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_1_ID)
                        .build())
                .build();
        // Scope to VM_2_ID only
        when(repositoryClient.getEntitiesByTypePerScope(any(), any()))
                .thenReturn(Stream.of(scopedOids));

        // Get the stats record for coverage
        final ReservedInstanceStatsRecord statsRecord =
                store.getReservedInstanceCoverageStats(filter, false,
                        Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());

        // Assertions
        assertThat(statsRecord.getCapacity().getTotal(), equalTo(100.0F));
        assertThat(statsRecord.getValues().getTotal(), equalTo(50.0F));
        assertThat(statsRecord.getSnapshotDate(), greaterThan(Instant.now().toEpochMilli()));
    }

    /**
     * Test that with multiple scope filters, an intersection of entities belonging to each scope
     * is returned.
     */
    @Test
    public void testRegionAndAccountScopeFilterNonEmptyIntersection() {
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        final ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_1_ID)
                        .build())
                .accountFilter(AccountFilter.newBuilder()
                        .addAccountId(ACCOUNT_ID)
                        .build())
                .build();
        when(repositoryClient.getEntitiesByTypePerScope(any(), any()))
                .thenCallRealMethod();
        when(repositoryClient.parseSupplyChainResponseToEntityOidsMap(any())).thenCallRealMethod();
        final List<GetMultiSupplyChainsResponse> responseCollection =
                Arrays.asList(createResponse(Collections.singletonList(VM_1_ID)),
                        createResponse(Arrays.asList(VM_1_ID, VM_2_ID)));
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(responseCollection);

        final Map<Long, EntityReservedInstanceCoverage> retCostMap =
                store.getScopedProjectedEntitiesRICoverages(filter);
        Assert.assertEquals(1, retCostMap.size());
        Assert.assertEquals(ENTITY_RI_COVERAGE, retCostMap.values().iterator().next());
    }

    /**
     * Test that with multiple scope filters, if the intersection of entities belonging to each
     * scope is an empty set, then an empty collection is returned.
     */
    @Test
    public void testRegionAndAccountScopeFilterEmptyIntersection() {
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        final ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_1_ID)
                        .build())
                .accountFilter(AccountFilter.newBuilder()
                        .addAccountId(ACCOUNT_ID)
                        .build())
                .build();
        when(repositoryClient.getEntitiesByTypePerScope(any(), any()))
                .thenCallRealMethod();
        when(repositoryClient.parseSupplyChainResponseToEntityOidsMap(any())).thenCallRealMethod();
        final List<GetMultiSupplyChainsResponse> responseCollection =
                Arrays.asList(createResponse(Collections.singletonList(VM_1_ID)),
                        createResponse(Collections.singletonList(VM_2_ID)));
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(responseCollection);
        final Map<Long, EntityReservedInstanceCoverage> retCostMap =
                store.getScopedProjectedEntitiesRICoverages(filter);
        Assert.assertTrue(retCostMap.isEmpty());
    }

    /**
     * Test that if no scoping filters are defined, then all the projected coverages are returned.
     */
    @Test
    public void testEmptyFilter() {
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        final ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .build();
        when(repositoryClient.getEntitiesByTypePerScope(any(), any()))
                .thenCallRealMethod();
        when(repositoryClient.parseSupplyChainResponseToEntityOidsMap(any())).thenCallRealMethod();
        final List<GetMultiSupplyChainsResponse> responseCollection =
                Arrays.asList(createResponse(Collections.singletonList(VM_1_ID)),
                        createResponse(Collections.singletonList(VM_2_ID)));
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(responseCollection);
        final Map<Long, EntityReservedInstanceCoverage> retCostMap =
                store.getScopedProjectedEntitiesRICoverages(filter);
        Assert.assertEquals(2, retCostMap.size());
    }

    /**
     * Test that if for the scope filters, there are no entities returned by the repository, then
     * the projected entities collection is empty.
     */
    @Test
    public void testEmptyEntityForScopes() {
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        final ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_1_ID)
                        .build())
                .build();
        when(repositoryClient.getEntitiesByTypePerScope(any(), any()))
                .thenCallRealMethod();
        when(repositoryClient.parseSupplyChainResponseToEntityOidsMap(any())).thenCallRealMethod();
        final List<GetMultiSupplyChainsResponse> responseCollection =
                Collections.singletonList(createResponse(Collections.emptyList()));
        when(supplyChainServiceMole.getMultiSupplyChains(any()))
                .thenReturn(responseCollection);

        final Map<Long, EntityReservedInstanceCoverage> retCostMap =
                store.getScopedProjectedEntitiesRICoverages(filter);
        Assert.assertTrue(retCostMap.isEmpty());
    }

    private GetMultiSupplyChainsResponse createResponse(final List<Long> memberOids) {
        return  GetMultiSupplyChainsResponse.newBuilder()
                .setSupplyChain(SupplyChain.newBuilder()
                        .addSupplyChainNodes(SupplyChainNode.newBuilder()
                                .setEntityType("VirtualMachine")
                                .putMembersByState(0, MemberList.newBuilder()
                                        .addAllMemberOids(memberOids)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    @Test
    public void testGetReservedInstanceCoverageStatsWithBuyRI() {

        // Store a map with coverage for both VM_1_ID and VM_2_ID
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        // Create a filter, the when below means we ignore the contents
        ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_1_ID)
                        .build())
                .build();

        // Scope to both VM_1 and VM_2 only
        final Map<EntityType, Set<Long>> scopedOids =
                ImmutableMap.of(EntityType.VIRTUAL_MACHINE, ImmutableSet.of(VM_1_ID, VM_2_ID),
                        EntityType.REGION, ImmutableSet.of(REGION_1_ID));
        when(repositoryClient.getEntitiesByTypePerScope(any(), any()))
                .thenReturn(Stream.of(scopedOids));

        // Get the stats record for coverage
        final ReservedInstanceStatsRecord statsRecord =
                store.getReservedInstanceCoverageStats(filter, true,
                        Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());

        // Assertions
        assertThat(statsRecord.getCapacity().getTotal(), equalTo(300.0F));
        assertThat(statsRecord.getValues().getTotal(), equalTo(275.0F));
        assertThat(statsRecord.getSnapshotDate(), greaterThan(Instant.now().toEpochMilli()));
    }

    @Test
    public void testGetReservedInstanceUtilizationStats() {

        final ReservedInstanceBought riCoveringVm2 = ReservedInstanceBought.newBuilder()
                .setId(4L)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                .setNumberOfCoupons(75)
                                .build()))
                .build();

        // setup input RI utilization filter
        final List<Long> scopeOids = Lists.newArrayList(3L, 11L);
        final ReservedInstanceUtilizationFilter riUtilizationFilter = ReservedInstanceUtilizationFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeOids)
                        .build())
                .build();

        // setup RI bought store
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(riCoveringVm2));

        // invoke SUT
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        final ReservedInstanceStatsRecord statsRecord =
                store.getReservedInstanceUtilizationStats(riUtilizationFilter, false,
                        Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());


        // Assertions
        assertThat(statsRecord.getCapacity().getTotal(), equalTo(75.0F));
        assertThat(statsRecord.getValues().getTotal(), equalTo(50.0F));
        assertThat(statsRecord.getSnapshotDate(), greaterThan(Instant.now().toEpochMilli()));
    }


    @Test
    public void testGetReservedInstanceUtilizationStatsWithBuyRI() {

        // Setup RI inventory
        final ReservedInstanceBought riCoveringVm2 = ReservedInstanceBought.newBuilder()
                .setId(4L)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                .setNumberOfCoupons(75)
                                .build()))
                .build();

        // Setup Buy RI
        final ReservedInstanceBought buyRICoveringVm2 = ReservedInstanceBought.newBuilder()
                .setId(7L)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                .setNumberOfCoupons(100)
                                .build()))
                .build();

        // setup input RI utilization filter
        final List<Long> scopeOids = Lists.newArrayList(3L, 11L);
        final ReservedInstanceUtilizationFilter riUtilizationFilter = ReservedInstanceUtilizationFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeOids)
                        .build())
                .build();

        // setup RI bought store
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(Lists.newArrayList(riCoveringVm2));

        // setup By RI store
        when(buyReservedInstanceStore.getBuyReservedInstances(any()))
                .thenReturn(Lists.newArrayList(buyRICoveringVm2));

        // invoke SUT
        store.updateProjectedRICoverage(topoInfo,
                Arrays.asList(ENTITY_RI_COVERAGE, SECOND_RI_COVERAGE));
        final ReservedInstanceStatsRecord statsRecord =
                store.getReservedInstanceUtilizationStats(riUtilizationFilter, true,
                        Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());


        // Assertions
        assertThat(statsRecord.getCapacity().getTotal(), equalTo(175.0F));
        assertThat(statsRecord.getValues().getTotal(), equalTo(75.0F));
        assertThat(statsRecord.getSnapshotDate(), greaterThan(Instant.now().toEpochMilli()));
    }
}