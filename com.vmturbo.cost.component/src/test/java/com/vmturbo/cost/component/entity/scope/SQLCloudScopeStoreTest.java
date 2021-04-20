package com.vmturbo.cost.component.entity.scope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.scheduling.TaskScheduler;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.cca.SQLComputeTierAllocationStore;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.savings.EntityState;
import com.vmturbo.cost.component.savings.EntityStateStore;
import com.vmturbo.cost.component.savings.SqlEntityStateStore;
import com.vmturbo.cost.component.savings.SqlEntityStateStoreTest;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class SQLCloudScopeStoreTest {


    private final Logger logger = LogManager.getLogger();

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

    private final DSLContext dsl = dbConfig.getDslContext();


    private final SQLCloudScopeStore cloudScopeStore = new SQLCloudScopeStore(
            dsl, mock(TaskScheduler.class), Duration.ZERO, 100, 100);

    /**
     * Entity State Store.
     */
    private final EntityStateStore entityStateStore = new SqlEntityStateStore(dsl, 100);;

    private final TopologyInfoTracker mockTopologyTracker = mock(TopologyInfoTracker.class);

    private final SQLComputeTierAllocationStore computeTierAllocationStore =
            new SQLComputeTierAllocationStore(dsl, mockTopologyTracker, MoreExecutors.newDirectExecutorService(),
                    1000, 1000, 1000);



    @Test
    public void testCleanup() throws EntitySavingsException {

        final Long entityOid1 = 1L;
        final Long entityOid2 = 7L;
        final ComputeTierAllocationDatapoint datapointA = ImmutableComputeTierAllocationDatapoint.builder()
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
        final ComputeTierAllocationDatapoint datapointB = ImmutableComputeTierAllocationDatapoint.builder()
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
        Set<EntityState> stateSet = ImmutableSet.of(new EntityState(entityOid1));
        TopologyEntityCloudTopology cloudTopology = SqlEntityStateStoreTest.getCloudTopology(1000L);
        entityStateStore.updateEntityStates(stateSet.stream().collect(
                Collectors.toMap(EntityState::getEntityId, Function.identity())), cloudTopology);

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
        final int numEntities = (int) Math.pow(10, 5);
        final int numEntitiesToShutdown = 2 * (int) Math.pow(10, 4);
        final int numEntitiesToUpdate = 5 * (int) Math.pow(10, 4);
        final int numEntitiesToDelete = 3 * (int) Math.pow(10, 4);



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
        long numCloudScopeRecordsRemoved = cloudScopeStore.cleanupCloudScopeRecords();
        cloudScopeCleanupStopwatch.stop();


        logger.info("First pass insertion into ECTA store completed (Duration={})", firstPassStopwatch);
        logger.info("Second pass insertion into ECTA store completed (Duration={})", firstPassStopwatch);
        logger.info("Allocation deletion completed (Duration={})", deletionStopwatch);
        logger.info("Cloud scope cleanup completed (Duration={}, Records Deleted={})",
                cloudScopeCleanupStopwatch, numCloudScopeRecordsRemoved);

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
}
