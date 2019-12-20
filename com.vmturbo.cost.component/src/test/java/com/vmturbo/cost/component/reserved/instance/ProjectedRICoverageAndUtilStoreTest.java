package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceFilter;
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
    private RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private GrpcTestServer testServer = GrpcTestServer.newServer(repositoryService);
    private SupplyChainServiceBlockingStub supplyChainService;
    private ProjectedRICoverageAndUtilStore store;
    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);
    private final Clock clock = Clock.systemUTC();

    private static final long VM_1_ID = 1L;
    private static final long VM_2_ID = 2L;
    private static final long REGION_1_ID = 3L;

    private static final EntityReservedInstanceCoverage ENTITY_RI_COVERAGE =
        EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(VM_1_ID)
                .setEntityCouponCapacity(200)
                .putCouponsCoveredByRi(10L, 100.0)
                .build();
    private static final EntityReservedInstanceCoverage SECOND_RI_COVERAGE =
                    EntityReservedInstanceCoverage.newBuilder()
                            .setEntityId(VM_2_ID)
                            .setEntityCouponCapacity(100)
                            .putCouponsCoveredByRi(4L, 50.0).build();
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
        supplyChainService = SupplyChainServiceGrpc.newBlockingStub(testServer.getChannel());
        store = Mockito.spy(new ProjectedRICoverageAndUtilStore(
                repositoryClient,
                supplyChainService,
                reservedInstanceBoughtStore,
                clock));
    }

    /**
     * Verify that we get back what we put in the map if we get all projected entities' coverage.
     */
    @Test
    public void testUpdateAndGet() {
        store.updateProjectedRICoverage(topoInfo, Arrays.asList(ENTITY_RI_COVERAGE));
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
        when(repositoryClient.getEntityOidsByType(any(), any(), any())).thenReturn(scopedOids);
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
        when(repositoryClient.getEntityOidsByType(any(), any(), any())).thenReturn(scopedOids);



        // Get the stats record for coverage
        final ReservedInstanceStatsRecord statsRecord =
                store.getReservedInstanceCoverageStats(filter);

        // Assertions
        assertThat(statsRecord.getCapacity().getTotal(), equalTo(100.0F));
        assertThat(statsRecord.getValues().getTotal(), equalTo(50.0F));
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

        // setup expected RI bought filter
        final ReservedInstanceBoughtFilter riBoughtFilter = ReservedInstanceBoughtFilter.newBuilder()
                .regionFilter(RegionFilter.newBuilder()
                        .addAllRegionId(scopeOids)
                        .build())
                .build();

        // setup RI bought store
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(eq(riBoughtFilter)))
                .thenReturn(Lists.newArrayList(riCoveringVm2));

    }
}
