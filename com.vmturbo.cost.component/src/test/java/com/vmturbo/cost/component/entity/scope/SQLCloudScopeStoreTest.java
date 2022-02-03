package com.vmturbo.cost.component.entity.scope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.cca.SQLComputeTierAllocationStore;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.savings.EntityState;
import com.vmturbo.cost.component.savings.EntityStateStore;
import com.vmturbo.cost.component.savings.SavingsUtil;
import com.vmturbo.cost.component.savings.SqlEntityStateStore;
import com.vmturbo.cost.component.savings.SqlEntityStateStoreTest;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

@RunWith(Parameterized.class)
public class SQLCloudScopeStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
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
    public SQLCloudScopeStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private final Logger logger = LogManager.getLogger();

    private SQLCloudScopeStore cloudScopeStore;

    /**
     * Entity State Store.
     */
    private EntityStateStore<DSLContext> entityStateStore;

    private final TopologyInfoTracker mockTopologyTracker = mock(TopologyInfoTracker.class);

    private SQLComputeTierAllocationStore computeTierAllocationStore;

    private final long vmOid1 = 101L;
    private final long vmOid2 = 102L;

    /**
     * Format of diagnostic dump files for pre-8.1.6 DB schema, with only 6 columns.
     */
    private final List<String> pre816SchemaLines = ImmutableList.of(
            String.format("[%d,73745724692990,73745724691229,73745724691226,73745724692991,\"2020-12-04T11:43:11\"]",
                    vmOid1),
            String.format("[%d,73745724692990,73745724691442,73745724691441,73745724692991,\"2020-12-04T11:43:12\"]",
                    vmOid2)
    );

    private final long vmOid3 = 201L;
    private final long volOid1 = 202L;
    private final long resourceGroupOid1 = 301L;

    /**
     * Format of diagnostic dump files for post-8.1.6 DB schema, with 8 columns.
     */
    private final List<String> post816SchemaLines = ImmutableList.of(
            String.format(
                    "[%d,10,73745724692990,73745724691229,73745724691226,73745724692991,null,\"2021-05-11T21:28:26\"]",
                    vmOid3),
            String.format(
                    "[%d,60,73953217476424,73953220080519,73953220080518,73953217476422,%d,\"2021-05-03T15:15:03\"]",
                    volOid1, resourceGroupOid1)
    );

    @Before
    public void before() {
        entityStateStore = new SqlEntityStateStore(dsl, 100);
        cloudScopeStore = new SQLCloudScopeStore(
                dsl, mock(TaskScheduler.class), Duration.ZERO, 100, 100);
        computeTierAllocationStore = new SQLComputeTierAllocationStore(dsl, mockTopologyTracker,
                MoreExecutors.newDirectExecutorService(),
                1000, 1000, 1000);
    }

    @Test
    public void testCleanup() throws EntitySavingsException {

        final Long entityOid1 = 1L;
        final Long entityOid2 = 7L;
        final ComputeTierAllocationDatapoint datapointA =
                ImmutableComputeTierAllocationDatapoint.builder()
                        .entityOid(entityOid1)
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
        final ComputeTierAllocationDatapoint datapointB =
                ImmutableComputeTierAllocationDatapoint.builder()
                        .entityOid(entityOid2)
                        .entityType(10)
                        .accountOid(8)
                        .regionOid(9)
                        .serviceProviderOid(10)
                        .cloudTierDemand(ComputeTierDemand.builder()
                                .osType(OSType.LINUX)
                                .tenancy(Tenancy.DEFAULT)
                                .cloudTierOid(12).build())
                        .build();

        final Set<ComputeTierAllocationDatapoint> datapoints = ImmutableSet.of(
                datapointA, datapointB);

        // Setup topology info
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyId(1)
                .setCreationTime(Instant.ofEpochMilli(12312312).toEpochMilli())
                .build();


        // Insert datapoints
        computeTierAllocationStore.persistAllocations(topologyInfo, datapoints);


        // Collect cloud scope records before deletion
        final Set<EntityCloudScope> cloudScopeSetBeforeDeletion = cloudScopeStore.streamAll()
                .collect(Collectors.toSet());

        // delete datapoint A
        final EntityComputeTierAllocationFilter deleteFilter = EntityComputeTierAllocationFilter.builder()
                .addEntityOids(1)
                .build();
        computeTierAllocationStore.deleteAllocations(deleteFilter);

        // Cleanup cloud scope records
        long numCloudScopeRecordsRemoved = cloudScopeStore.cleanupCloudScopeRecords();

        final Set<EntityCloudScope> cloudScopeSetAfterDeletion = cloudScopeStore.streamAll()
                .collect(Collectors.toSet());


        final EntityCloudScope entityCloudScopeA = EntityCloudScope.builder()
                .entityOid(datapointA.entityOid())
                .accountOid(datapointA.accountOid())
                .regionOid(datapointA.regionOid())
                .availabilityZoneOid(datapointA.availabilityZoneOid())
                .serviceProviderOid(datapointA.serviceProviderOid())
                // creation time is not used for comparison
                .creationTime(Instant.now())
                .build();

        final EntityCloudScope entityCloudScopeB = EntityCloudScope.builder()
                .entityOid(datapointB.entityOid())
                .accountOid(datapointB.accountOid())
                .regionOid(datapointB.regionOid())
                .availabilityZoneOid(datapointB.availabilityZoneOid())
                .serviceProviderOid(datapointB.serviceProviderOid())
                .creationTime(Instant.now())
                .build();

        assertThat(cloudScopeSetBeforeDeletion, hasSize(2));
        assertThat(cloudScopeSetBeforeDeletion, containsInAnyOrder(entityCloudScopeA, entityCloudScopeB));

        assertThat(numCloudScopeRecordsRemoved, equalTo(1L));
        assertThat(cloudScopeSetAfterDeletion, hasSize(1));
        assertThat(cloudScopeSetAfterDeletion, containsInAnyOrder(entityCloudScopeB));


        //
        // Verify that the records entity_cloud_scope table won't be delete if it referenced by a
        // table other than entity_compute_tier_allocation.
        //
        // Add datapointA back
        computeTierAllocationStore.persistAllocations(topologyInfo, ImmutableSet.of(datapointA));

        // Add an entity state for the same entity for datapointA.
        // The record that correspond to entityOid1 in the cloud scope table will be referenced by
        // two tables.
        Set<EntityState> stateSet = ImmutableSet.of(new EntityState(entityOid1,
                SavingsUtil.EMPTY_PRICE_CHANGE));
        TopologyEntityCloudTopology cloudTopology = SqlEntityStateStoreTest.getCloudTopology(1000L);
        entityStateStore.updateEntityStates(stateSet.stream().collect(
                Collectors.toMap(EntityState::getEntityId, Function.identity())), cloudTopology, dsl,
                Collections.emptySet());

        // Delete datapoint A from entity_compute_tier_allocation table.
        computeTierAllocationStore.deleteAllocations(deleteFilter);

        // Cleanup cloud scope records
        numCloudScopeRecordsRemoved = cloudScopeStore.cleanupCloudScopeRecords();

        final Set<EntityCloudScope> cloudScopeSetAfterDeletion2ndTime = cloudScopeStore.streamAll()
                .collect(Collectors.toSet());

        assertThat(numCloudScopeRecordsRemoved, equalTo(0L));
        assertThat(cloudScopeSetAfterDeletion2ndTime, hasSize(2));
        List<Long> entityOidsInScopeTable = cloudScopeSetAfterDeletion2ndTime.stream()
                .map(EntityCloudScope::entityOid).collect(Collectors.toList());
        assertThat(entityOidsInScopeTable, containsInAnyOrder(entityOid1, entityOid2));
    }

    @Ignore
    @Test
    public void testCleanupPerformance() {


        final int numAccounts = 1000;
        final int numEntities = (int)Math.pow(10, 5);
        final int numEntitiesToShutdown = 2 * (int)Math.pow(10, 4);
        final int numEntitiesToUpdate = 5 * (int)Math.pow(10, 4);
        final int numEntitiesToDelete = 3 * (int)Math.pow(10, 4);



        final Set<ComputeTierAllocationDatapoint> firstPassDatapoints = new HashSet<>();

        IntStream.range(0, numEntities).forEach(i ->
                firstPassDatapoints.add(createDatapoint(i, numAccounts)));


        // Setup fist pass topology info
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyId(1)
                .setCreationTime(Instant.ofEpochMilli(123).toEpochMilli())
                .build();


        final Stopwatch firstPassStopwatch = Stopwatch.createStarted();
        computeTierAllocationStore.persistAllocations(topologyInfo, firstPassDatapoints);
        firstPassStopwatch.stop();


        // Setup the second pass insertion
        final List<ComputeTierAllocationDatapoint> secondPassDatapoints = Lists.newArrayList(firstPassDatapoints);

        Collections.shuffle(secondPassDatapoints);

        // remove some of the datapoints (Simulate entities that shut down)
        IntStream.range(0, numEntitiesToShutdown).forEach(i -> secondPassDatapoints.remove(0));

        // update some of the entities with new allocation demand
        IntStream.range(0, numEntitiesToUpdate).forEach(i ->
                secondPassDatapoints.set(i,
                        ImmutableComputeTierAllocationDatapoint.copyOf(secondPassDatapoints.get(i))
                                .withCloudTierDemand(ComputeTierDemand.builder()
                                        .osType(OSType.LINUX)
                                        .tenancy(Tenancy.DEFAULT)
                                        .cloudTierOid(2L)
                                        .build())));


        // Setup second topology info
        final TopologyInfo secondTopologyInfo = TopologyInfo.newBuilder()
                .setTopologyId(1)
                .setCreationTime(Instant.ofEpochMilli(456).toEpochMilli())
                .build();

        when(mockTopologyTracker.getPriorTopologyInfo(secondTopologyInfo)).thenReturn(Optional.of(topologyInfo));

        // Insert the second pass allocations
        final Stopwatch secondPassStopwatch = Stopwatch.createStarted();
        computeTierAllocationStore.persistAllocations(secondTopologyInfo, secondPassDatapoints);
        secondPassStopwatch.stop();


        // delete a random set of original datapoints
        final List<ComputeTierAllocationDatapoint> datapointsToRemove = Lists.newArrayList(firstPassDatapoints);
        Collections.shuffle(datapointsToRemove);

        final EntityComputeTierAllocationFilter filter = EntityComputeTierAllocationFilter.builder()
                .entityOids(datapointsToRemove.subList(0, numEntitiesToDelete).stream()
                        .map(ComputeTierAllocationDatapoint::entityOid)
                        .collect(Collectors.toSet()))
                .endTimeFilter(TimeFilter.builder()
                        .comparator(TimeComparator.EQUAL_TO)
                        .time(Instant.ofEpochMilli(topologyInfo.getCreationTime()))
                        .build())
                .build();

        final Stopwatch deletionStopwatch = Stopwatch.createStarted();
        computeTierAllocationStore.deleteAllocations(filter);
        deletionStopwatch.stop();


        final Stopwatch cloudScopeCleanupStopwatch = Stopwatch.createStarted();
        cloudScopeCleanupStopwatch.stop();

        logger.info("First pass insertion into ECTA store completed (Duration={})", firstPassStopwatch);
        logger.info("Second pass insertion into ECTA store completed (Duration={})", firstPassStopwatch);
        logger.info("Allocation deletion completed (Duration={})", deletionStopwatch);
        logger.info("Cloud scope cleanup completed (Duration={}, Records Deleted={})",
                cloudScopeCleanupStopwatch, cloudScopeStore.cleanupCloudScopeRecords());

    }


    private ComputeTierAllocationDatapoint createDatapoint(long id,
                                                long numAccounts) {

        return ImmutableComputeTierAllocationDatapoint.builder()
                .entityOid(id)
                .accountOid(Math.round(Math.random() * numAccounts))
                .regionOid(Math.round(Math.random() * 100))
                .availabilityZoneOid(Math.round(Math.random() * 300))
                .serviceProviderOid(Math.round(Math.random() * 3))
                .cloudTierDemand(
                        // Allocation demand does not effect cloud scope
                        ComputeTierDemand.builder()
                                .osType(OSType.LINUX)
                                .tenancy(Tenancy.DEFAULT)
                                .cloudTierOid(1L)
                                .build())
                .build();


    }

    /**
     * Tests restoration of DB records from pre-8.1.6 schema with only 6 DB columns.
     */
    @Test
    public void restorePre816Diagnostics() {
        assertEquals(2, pre816SchemaLines.size());
        final List<EntityCloudScopeRecord> entriesBefore = getScopeRecords();
        assertTrue(entriesBefore.isEmpty());

        cloudScopeStore.restoreDiags(pre816SchemaLines.stream(), null);

        final List<EntityCloudScopeRecord> entriesAfter = getScopeRecords();
        assertEquals(2, entriesAfter.size());

        final EntityCloudScopeRecord scope1 = entriesAfter.get(0);
        assertEquals(vmOid1, scope1.getEntityOid().longValue());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, scope1.getEntityType().intValue());
        assertNull(scope1.getResourceGroupOid());

        final EntityCloudScopeRecord scope2 = entriesAfter.get(1);
        assertEquals(vmOid2, scope2.getEntityOid().longValue());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, scope2.getEntityType().intValue());
        assertNull(scope2.getResourceGroupOid());
    }

    /**
     * Tests post-8.1.6 DB record restoration to make sure that still works.
     */
    @Test
    public void restorePost816Diagnostics() {
        assertEquals(2, post816SchemaLines.size());
        final List<EntityCloudScopeRecord> entriesBefore = getScopeRecords();
        assertTrue(entriesBefore.isEmpty());

        cloudScopeStore.restoreDiags(post816SchemaLines.stream(), null);

        final List<EntityCloudScopeRecord> entriesAfter = getScopeRecords();
        assertEquals(2, entriesAfter.size());

        final EntityCloudScopeRecord scope1 = entriesAfter.get(0);
        assertEquals(vmOid3, scope1.getEntityOid().longValue());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, scope1.getEntityType().intValue());
        assertNull(scope1.getResourceGroupOid());

        final EntityCloudScopeRecord scope2 = entriesAfter.get(1);
        assertEquals(volOid1, scope2.getEntityOid().longValue());
        assertEquals(EntityType.VIRTUAL_VOLUME_VALUE, scope2.getEntityType().intValue());
        assertEquals(resourceGroupOid1, scope2.getResourceGroupOid().longValue());
    }

    /**
     * Convenience method to query scope DB records, to help with test verification.
     *
     * @return Scope DB records in asc order of entity OIDs.
     */
    @Nonnull
    private List<EntityCloudScopeRecord> getScopeRecords() {
        return dsl.selectFrom(Tables.ENTITY_CLOUD_SCOPE)
                .orderBy(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID)
                .stream()
                .collect(Collectors.toList());
    }
}
