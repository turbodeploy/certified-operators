package com.vmturbo.cost.component.cca;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

@RunWith(Parameterized.class)
public class SQLComputeTierAllocationStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SQLComputeTierAllocationStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private final TopologyInfoTracker mockTopologyTracker = mock(TopologyInfoTracker.class);

    private SQLComputeTierAllocationStore computeTierAllocationStore;


    /*
    The baseline configuration of the DB
     */

    private final ComputeTierAllocationDatapoint baselineDatapointA = ImmutableComputeTierAllocationDatapoint.builder()
            .entityOid(1)
            .entityType(10)
            .accountOid(2)
            .regionOid(3)
            .availabilityZoneOid(4)
            .serviceProviderOid(5)
            .cloudTierDemand(ComputeTierDemand.builder()
                    .osType(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .cloudTierOid(6).build())
            .build();
    final ComputeTierAllocationDatapoint baselineDatapointB = ImmutableComputeTierAllocationDatapoint.builder()
            .entityOid(7)
            .entityType(10)
            .accountOid(8)
            .regionOid(9)
            .serviceProviderOid(10)
            .cloudTierDemand(ComputeTierDemand.builder()
                    .osType(OSType.WINDOWS)
                    .tenancy(Tenancy.HOST)
                    .cloudTierOid(12).build())
            .build();

    final ComputeTierAllocationDatapoint baselineDatapointC = ImmutableComputeTierAllocationDatapoint.builder()
            .entityOid(11)
            .entityType(10)
            .accountOid(8)
            .regionOid(9)
            .serviceProviderOid(10)
            .cloudTierDemand(ComputeTierDemand.builder()
                    .osType(OSType.WINDOWS)
                    .tenancy(Tenancy.DEFAULT)
                    .cloudTierOid(12).build())
            .build();


    final Set<ComputeTierAllocationDatapoint> baselineDatapoints = ImmutableSet.of(
            baselineDatapointA, baselineDatapointB, baselineDatapointC);

        /*
        Setup topology info
         */

    final Instant baselineCreationTime = Instant.ofEpochMilli(123012);
    final TopologyInfo baselineTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(baselineCreationTime.toEpochMilli())
            .build();

    @Before
    public void setup() {
        computeTierAllocationStore = new SQLComputeTierAllocationStore(dsl, mockTopologyTracker,
                MoreExecutors.newDirectExecutorService(), 1000, 1000, 1000);
        when(mockTopologyTracker.getPriorTopologyInfo(eq(baselineTopologyInfo))).thenReturn(
                Optional.empty());

        /*
        Invoke store
         */
        computeTierAllocationStore.persistAllocations(baselineTopologyInfo, baselineDatapoints);
    }

    @Test
    public void testCleanInsert() {

        // Records are inserted as part of the JUnit setup

        final Set<EntityComputeTierAllocation> allocations = computeTierAllocationStore
                .streamAllocations(EntityComputeTierAllocationFilter.ALL)
                .collect(Collectors.toSet());

        assertThat(allocations, hasSize(3));

        final EntityComputeTierAllocation allocationA = datapointToAllocation(baselineDatapointA, baselineCreationTime);
        final EntityComputeTierAllocation allocationB = datapointToAllocation(baselineDatapointB, baselineCreationTime);
        final EntityComputeTierAllocation allocationC = datapointToAllocation(baselineDatapointC, baselineCreationTime);


        assertThat(allocations, containsInAnyOrder(allocationA, allocationB, allocationC));
    }

    @Test
    public void testInsertAndUpdate() {

        // Datapoint A remains the same
        final ComputeTierAllocationDatapoint datapointA = baselineDatapointA;

        // Datapoint B changes its cloud tier
        final ComputeTierAllocationDatapoint datapointB = ImmutableComputeTierAllocationDatapoint.copyOf(baselineDatapointB)
                .withCloudTierDemand(ComputeTierDemand.builder()
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .cloudTierOid(13).build());

        // A new entity is introduced and datapoint C is skipped
        final ComputeTierAllocationDatapoint datapointD = ImmutableComputeTierAllocationDatapoint.copyOf(baselineDatapointC)
                .withEntityOid(14);

        final Set<ComputeTierAllocationDatapoint> allocationDatapoints = ImmutableSet.of(
                datapointA, datapointB, datapointD);

        // Setup topology info
        final Instant creationTime = baselineCreationTime.plus(10, ChronoUnit.MINUTES);
        final TopologyInfo topologyInfo = baselineTopologyInfo.toBuilder()
                .setCreationTime(creationTime.toEpochMilli())
                .setTopologyId(baselineTopologyInfo.getTopologyId() + 1)
                .build();
        when(mockTopologyTracker.getPriorTopologyInfo(eq(topologyInfo))).thenReturn(Optional.of(baselineTopologyInfo));

        // Persist the allocation datapoints
        computeTierAllocationStore.persistAllocations(topologyInfo, allocationDatapoints);


        final Set<EntityComputeTierAllocation> allocations = computeTierAllocationStore
                .streamAllocations(EntityComputeTierAllocationFilter.ALL)
                .collect(Collectors.toSet());

        // A should have 1 record, B should have 2, C 1, and D 1 = 5
        assertThat(allocations, hasSize(5));

        final EntityComputeTierAllocation allocationA = datapointToAllocation(baselineDatapointA, baselineCreationTime, creationTime);
        // The baseline allocation for B
        final EntityComputeTierAllocation allocationB1 = datapointToAllocation(baselineDatapointB, baselineCreationTime);
        // The new allocation for B
        final EntityComputeTierAllocation allocationB2 = datapointToAllocation(datapointB, creationTime);
        final EntityComputeTierAllocation allocationC = datapointToAllocation(baselineDatapointC, baselineCreationTime);
        final EntityComputeTierAllocation allocationD = datapointToAllocation(datapointD, creationTime);


        assertThat(allocations, containsInAnyOrder(allocationA, allocationB1, allocationB2, allocationC, allocationD));
    }

    @Test
    public void testUpdateAndDelete() {

        // Datapoint A remains the same
        final ComputeTierAllocationDatapoint datapointA = baselineDatapointA;

        // Datapoint B changes its cloud tier
        final ComputeTierAllocationDatapoint datapointB = ImmutableComputeTierAllocationDatapoint.copyOf(baselineDatapointB)
                .withCloudTierDemand(ComputeTierDemand.builder()
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .cloudTierOid(13).build());

        // A new entity is introduced and datapoint C is skipped
        final ComputeTierAllocationDatapoint datapointD = ImmutableComputeTierAllocationDatapoint.copyOf(baselineDatapointC)
                .withEntityOid(14);

        final Set<ComputeTierAllocationDatapoint> allocationDatapoints = ImmutableSet.of(
                datapointA, datapointB, datapointD);

        // Setup topology info
        final Instant creationTime = baselineCreationTime.plus(10, ChronoUnit.MINUTES);
        final TopologyInfo topologyInfo = baselineTopologyInfo.toBuilder()
                .setCreationTime(creationTime.toEpochMilli())
                .setTopologyId(baselineTopologyInfo.getTopologyId() + 1)
                .build();
        when(mockTopologyTracker.getPriorTopologyInfo(eq(topologyInfo))).thenReturn(Optional.of(baselineTopologyInfo));

        // Persist the allocation datapoints
        computeTierAllocationStore.persistAllocations(topologyInfo, allocationDatapoints);


        // Delete records where end_time = baseline topology creation time
        final EntityComputeTierAllocationFilter filter = EntityComputeTierAllocationFilter.builder()
                .endTimeFilter(TimeFilter.builder()
                        .comparator(TimeComparator.EQUAL_TO)
                        .time(baselineCreationTime)
                        .build())
                .build();

        computeTierAllocationStore.deleteAllocations(filter);

        final Set<EntityComputeTierAllocation> allocations = computeTierAllocationStore
                .streamAllocations(EntityComputeTierAllocationFilter.ALL)
                .collect(Collectors.toSet());

        // Only the new records should exist (A, new B, and D)
        assertThat(allocations, hasSize(3));

        final EntityComputeTierAllocation allocationA = datapointToAllocation(baselineDatapointA, baselineCreationTime, creationTime);
        // The new allocation for B
        final EntityComputeTierAllocation allocationB = datapointToAllocation(datapointB, creationTime);
        final EntityComputeTierAllocation allocationD = datapointToAllocation(datapointD, creationTime);


        assertThat(allocations, containsInAnyOrder(allocationA, allocationB, allocationD));
    }

    @Test
    public void testPlatformFilter() {

        final EntityComputeTierAllocationFilter filter = EntityComputeTierAllocationFilter.builder()
                .addPlatforms(OSType.WINDOWS)
                .build();

        final Set<EntityComputeTierAllocation> allocations = computeTierAllocationStore
                .streamAllocations(filter)
                .collect(Collectors.toSet());

        // Datapoints B and C should be returned
        assertThat(allocations, hasSize(2));

        final EntityComputeTierAllocation allocationB = datapointToAllocation(baselineDatapointB, baselineCreationTime);
        final EntityComputeTierAllocation allocationC = datapointToAllocation(baselineDatapointC, baselineCreationTime);

        assertThat(allocations, containsInAnyOrder(allocationB, allocationC));

    }

    @Test
    public void testTenancyFilter() {

        final EntityComputeTierAllocationFilter filter = EntityComputeTierAllocationFilter.builder()
                .addTenancies(Tenancy.DEFAULT)
                .build();

        final Set<EntityComputeTierAllocation> allocations = computeTierAllocationStore
                .streamAllocations(filter)
                .collect(Collectors.toSet());

        // Datapoints A and C should be returned
        assertThat(allocations, hasSize(2));

        final EntityComputeTierAllocation allocationA = datapointToAllocation(baselineDatapointA, baselineCreationTime);
        final EntityComputeTierAllocation allocationC = datapointToAllocation(baselineDatapointC, baselineCreationTime);

        assertThat(allocations, containsInAnyOrder(allocationA, allocationC));

    }

    @Test
    public void testComputeTierFilter() {

        final EntityComputeTierAllocationFilter filter = EntityComputeTierAllocationFilter.builder()
                .addComputeTierOids(12)
                .build();

        final Set<EntityComputeTierAllocation> allocations = computeTierAllocationStore
                .streamAllocations(filter)
                .collect(Collectors.toSet());

        // Datapoints A and C should be returned
        assertThat(allocations, hasSize(2));

        final EntityComputeTierAllocation allocationB = datapointToAllocation(baselineDatapointB, baselineCreationTime);
        final EntityComputeTierAllocation allocationC = datapointToAllocation(baselineDatapointC, baselineCreationTime);

        assertThat(allocations, containsInAnyOrder(allocationB, allocationC));

    }


    private EntityComputeTierAllocation datapointToAllocation(
            @Nonnull ComputeTierAllocationDatapoint allocationDatapoint,
            @Nonnull Instant startEndTime) {
        return datapointToAllocation(allocationDatapoint, startEndTime, startEndTime);
    }

    private EntityComputeTierAllocation datapointToAllocation(
            @Nonnull ComputeTierAllocationDatapoint allocationDatapoint,
            @Nonnull Instant startTime,
            @Nonnull Instant endTime) {

        return EntityComputeTierAllocation.builder()
                .entityOid(allocationDatapoint.entityOid())
                .accountOid(allocationDatapoint.accountOid())
                .regionOid(allocationDatapoint.regionOid())
                .availabilityZoneOid(allocationDatapoint.availabilityZoneOid())
                .serviceProviderOid(allocationDatapoint.serviceProviderOid())
                .timeInterval(TimeInterval.builder()
                        .startTime(startTime)
                        .endTime(endTime)
                        .build())
                .cloudTierDemand(allocationDatapoint.cloudTierDemand())
                .build();

    }

}
