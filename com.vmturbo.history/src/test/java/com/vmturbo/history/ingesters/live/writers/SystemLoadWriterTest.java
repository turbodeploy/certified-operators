package com.vmturbo.history.ingesters.live.writers;

import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.jdbcx.JdbcDataSource;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Tests of the {@link SystemLoadWriter} class.
 */
public class SystemLoadWriterTest {
    private static final Logger logger = LogManager.getLogger();

    private static final String DAY1_245PM = "2020-01-01T14:25:00Z";
    private static final String DAY1_345PM = "2020-01-01T15:25:00Z";


    private static final Set<SystemLoadCommodity> hostCommodities = ImmutableSet.of(
            SystemLoadCommodity.CPU, SystemLoadCommodity.CPU_PROVISIONED,
            SystemLoadCommodity.MEM, SystemLoadCommodity.MEM_PROVISIONED,
            SystemLoadCommodity.VCPU, SystemLoadCommodity.VMEM
    );

    private static final Set<SystemLoadCommodity> storageCommodities = ImmutableSet.of(
            SystemLoadCommodity.IO_THROUGHPUT, SystemLoadCommodity.NET_THROUGHPUT,
            SystemLoadCommodity.STORAGE_AMOUNT, SystemLoadCommodity.STORAGE_PROVISIONED,
            SystemLoadCommodity.STORAGE_ACCESS
    );

    // Define entities and clusters for use in tests.
    // Numeric values are "base" values for capacities, usages, and peaks. Actual values in
    // bought/sold commodity values set in the entity for individual system load commodities are
    // computed as the base value times ordinal+1 of the system load commodity value.

    // N.B. The base values used here are easy to use, but they're unrealistic in that we end
    // up calculating utilizations that are > 1. That would indicate a bug in real life, but the
    // stuff we're testing here is unaffected.

    private static final long PM_ID_BASE = 100L;
    private static final long PM1_ID = PM_ID_BASE + 1;
    private static final TopologyEntityDTO PM1 = createPm(PM1_ID, 1);
    private static final long PM2_ID = PM_ID_BASE + 2;
    private static final TopologyEntityDTO PM2 = createPm(PM2_ID, 2);
    private static final long PM3_ID = PM_ID_BASE + 3;
    private static final TopologyEntityDTO PM3 = createPm(PM3_ID, 3);

    private static final long STG_ID_BASE = 200L;
    private static final long STG1_ID = STG_ID_BASE + 1;
    private static final TopologyEntityDTO STG1 = createStorage(STG1_ID, 1, PM1);
    private static final long STG2_ID = STG_ID_BASE + 2;
    private static final TopologyEntityDTO STG2 = createStorage(STG2_ID, 2, PM2, PM3);

    private static final long VM_ID_BASE = 300L;
    private static final long VM1_ID = VM_ID_BASE + 1;
    private static final TopologyEntityDTO VM1 = createVm(VM1_ID, 2, 3, 4, PM1);
    private static final long VM2_ID = VM_ID_BASE + 2;
    private static final TopologyEntityDTO VM2 = createVm(VM2_ID, 5, 6, 7, PM2);
    private static final long VM3_ID = VM_ID_BASE + 3;
    private static final TopologyEntityDTO VM3 = createVm(VM3_ID, 3, 4, 5, PM3);
    private static final long VM4_ID = VM_ID_BASE + 4;
    private static final TopologyEntityDTO VM4 = createVm(VM4_ID, 4, 5, 6, PM3);

    private static final long CLUSTER_ID_BASE = 1000L;
    private static final long CLUSTER1_ID = CLUSTER_ID_BASE + 1;
    private static final Grouping CLUSTER1 = createCluster(CLUSTER1_ID, PM1_ID, STG1_ID);
    private static final long CLUSTER2_ID = CLUSTER_ID_BASE + 2;
    private static final Grouping CLUSTER2 = createCluster(CLUSTER2_ID, PM2_ID, PM3_ID, STG2_ID);

    private static final String TOPOLOGY_SUMMARY = "test topology";

    private SystemLoadWriter.Factory writerFactory;

    private static DSLContext dsl;
    private static HistorydbIO historydbIO;
    private static final ExecutorService threadPool = MoreExecutors.newDirectExecutorService();
    private SimpleBulkLoaderFactory loaders;

    private static final BulkInserterConfig config = ImmutableBulkInserterConfig.builder()
            .batchSize(2)
            .maxPendingBatches(2)
            .maxBatchRetries(3)
            .maxRetryBackoffMsec(1000)
            .build();

    /**
     * Set up for tests, including establishing an in-memory H2 database that will be resued for all
     * tests.
     *
     * <p>By using jOOQ's internal schema netadata to build the schema, we avoid the growing cost
     * of doing that using Flyway migrations. And of course we get a big speed boost by using an
     * in-memory DB.</p>
     *
     * <p>We also set up a HistorydbIO mock that's customized to skip its normal initialization and
     * and deliver H2 connections via its various connection-providing methods.</p>
     *
     * @throws SQLException   if we have a DB problem
     * @throws VmtDbException thrown by some mocked HistorydbIO methods
     */
    @BeforeClass
    public static void beforeClass() throws SQLException, VmtDbException {
        final JdbcDataSource ds = new JdbcDataSource();
        ds.setUrl("jdbc:h2:mem:testdb;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("sa");
        dsl = DSL.using(ds, SQLDialect.H2);
        Stopwatch w = Stopwatch.createStarted();
        dsl.ddl(Vmtdb.VMTDB).forEach(Query::execute);
        logger.info("Created in {}", w);
        historydbIO = mock(HistorydbIO.class);
        HistorydbIO.setSharedInstance(historydbIO);
        when(historydbIO.connection()).thenAnswer(i -> ds.getConnection());
        when(historydbIO.transConnection()).thenAnswer(i -> ds.getConnection());
        when(historydbIO.unpooledConnection()).thenAnswer(i -> ds.getConnection());
        when(historydbIO.unpooledTransConnection()).thenAnswer(i -> ds.getConnection());
        when(historydbIO.using(any(Connection.class))).thenReturn(dsl);
    }

    /**
     * Set up temporary database for use in tests.
     *
     * <p>We only do this for the first test that runs, even though it's in the @Before method.
     * Tables that are actually changed by any given tests are cleared out after that test, which is
     * much lest costly than tearing the database down and recreating it.</p>
     *
     * @throws IOException if there's a problem starting up group service
     */
    @Before
    public void before() throws IOException {
        final GroupServiceBlockingStub groupService = createGroupService();
        Stopwatch w = Stopwatch.createStarted();
        dsl.connection(c -> {
            DSL.using(c).execute("SET REFERENTIAL_INTEGRITY=false");
            Vmtdb.VMTDB.tableStream().forEach(t -> DSL.using(c).truncateTable(t).execute());
        });
        logger.info("Truncated all in {}", w);
        loaders = new SimpleBulkLoaderFactory(historydbIO, config, threadPool);
        this.writerFactory = new SystemLoadWriter.Factory(groupService, historydbIO);
    }

    /**
     * Make sure all our bulk loaders are closed and flushed after each test.
     *
     * @throws InterruptedException if interrupted
     */
    @After
    public void after() throws InterruptedException {
        loaders.close();
    }

    /**
     * Shut down the threadpool and destroy the test database when finished.
     */
    @AfterClass
    public static void afterClass() {
        threadPool.shutdownNow();
        dsl.dropSchema(Vmtdb.VMTDB);
    }

    /** manages release of grpc test resources at end of tests. */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    /**
     * Test that an processing a topology with no entities that contribute to a cluster results
     * in utilization records for that cluster that show zero values for capacities and usages
     * of all system load commodities.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testEmptyClusterUtilizations() throws InterruptedException {
        SystemLoadWriter systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_245PM), loaders).get();
        // none of the entities touches cluster 2
        systemLoadWriter.processEntities(createChunk(PM1, STG1, VM1), TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);
        // cluster 2 capacities and usages should all be 0, as should the overall system load
        checkUtilizationRecords(DAY1_245PM, CLUSTER2_ID, 0.0, 0.0, 0.0, 0.0, 0.0);
    }

    /**
     * Check that a simple topology with one PM, one VM, and one STORAGE, all contributing to a
     * single cluster, leaves that cluster with correct capacities, usages, and system load.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testSimpleClusterUtilizations() throws InterruptedException {
        SystemLoadWriter systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_245PM), loaders).get();
        systemLoadWriter.processEntities(createChunk(PM1, STG1, VM1), TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);
        // all capacity bases are 1.0, and usage bases are all 2.0 in this cluster, so utilization
        // is 2.0 across the board
        checkUtilizationRecords(DAY1_245PM, CLUSTER1_ID, 1.0, 1.0, 2.0, 2.0, 2.0);
    }

    /**
     * Check that a more complex topology with multiple PM, VM and STORAGE entities contributing
     * to the same cluster reults in the correct capacities, usages, and system load for that
     * cluster.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testComplexClusterUtilizations() throws InterruptedException {
        SystemLoadWriter systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_245PM), loaders).get();
        systemLoadWriter.processEntities(createChunk(PM2, STG2, VM3, PM3, VM2), TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);
        // PM2 and PM3 sell into cluster 2 with base capacities 2 & 3, so aggregate is 5.
        // STG sells to both PM1 an PM2, so stg capacity is 2.
        // VM2 buys from PM2 with base used 5, and VM3 buys from PM3 with base used 3, so aggregate used is 8.
        // utilization maxes out at 4 in stg commodities.
        checkUtilizationRecords(DAY1_245PM, CLUSTER2_ID, 5.0, 2.0, 8.0, 8.0, 4.0);
    }

    /**
     * Check that if entities of types that cannot contribute to system load are included in the
     * topology, they have no impact on the recorded utilizations.
     *
     * @throws InterruptedException if  interrupted
     */
    @Test
    public void testOtherEntityTypesDontMatter() throws InterruptedException {
        SystemLoadWriter systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_245PM), loaders).get();
        TopologyEntityDTO loadBalancer = TopologyEntityDTO.newBuilder()
                .mergeFrom(VM1)
                .setEntityType(EntityType.LOAD_BALANCER_VALUE)
                .build();
        systemLoadWriter.processEntities(createChunk(PM1, STG1, VM1, loadBalancer), TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);
        // all capacity bases are 1.0, and usage bases are all 2.0 in this cluster, so utilization
        // is 2.0 across the board
        checkUtilizationRecords(DAY1_245PM, CLUSTER1_ID, 1.0, 1.0, 2.0, 2.0, 2.0);
    }

    /**
     * Check that if entities that contribute to system load have bought or sold commodities
     * ohter than system load commodities, those do not affect the calculated utilizations.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testOtherCommodityTypesDontMatter() throws InterruptedException {
        // create customized copies of PM1 and VM1 that include nonzero capacities/usages
        // (respectively) for all non-system-load commodities
        SystemLoadWriter systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_245PM), loaders).get();
        final List<TopologyEntityDTO> chunk = createChunk(
                getExtendedEntity(PM1), getExtendedEntity(STG1), getExtendedEntity(VM1));
        systemLoadWriter.processEntities(chunk, TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);

        // all capacity bases are 1.0, and usage bases are all 2.0 in this cluster, so utilization
        // is 2.0 across the board
        checkUtilizationRecords(DAY1_245PM, CLUSTER1_ID, 1.0, 1.0, 2.0, 2.0, 2.0);
    }

    /**
     * Check that system load commodities bought and sold by VMs are recorded for the affected
     * clusters.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testVmCommoditiesAreRecorded() throws InterruptedException {
        SystemLoadWriter systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_245PM), loaders).get();
        systemLoadWriter.processEntities(createChunk(VM1), TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);
        checkEntityCommodityRecords(DAY1_245PM, CLUSTER1_ID, VM1_ID, 2, 3, 4);
    }

    /**
     * Test that when prior system load values for the current date are available, newly computed
     * values are recorded in the database only for clusters whose new system load values exceed
     * the currently recorded values.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testSliceReWrittenOnlyOnLoadIncrease() throws InterruptedException {
        // first load a full topology touching both clusters
        SystemLoadWriter systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_245PM), loaders).get();
        systemLoadWriter.processEntities(createChunk(PM1, PM2, PM3, STG1, STG2, VM1, VM2, VM3), TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);
        // now we run another cycle, using VM4 instead of VM3 - only difference is higher usages,
        // so utilization for cluster 2 should go up
        systemLoadWriter = (SystemLoadWriter)writerFactory.getChunkProcessor(
                createTopoInfo(DAY1_345PM), loaders).get();
        systemLoadWriter.processEntities(createChunk(PM1, PM2, PM3, STG1, STG2, VM1, VM2, VM4), TOPOLOGY_SUMMARY);
        systemLoadWriter.finish(1, false, TOPOLOGY_SUMMARY);
        // make sure we had utilization records written for cluster 2 but not cluster 1
        assertThat(getUtilizationRecordClusters(DAY1_345PM), is(ImmutableSet.of(CLUSTER2_ID)));
        // there's no similar check we can take re commodity records, cuz those are written for all
        //  clusters to the transient table before the writer determins which slices need to be recorded
    }

    /**
     * Get the set of cluster ids from which utilization records were produced.
     *
     * @param snapshotTime timestamp of records to include
     * @return set of cluster ids
     */
    private Set<Long> getUtilizationRecordClusters(final String snapshotTime) {
        Timestamp t = Timestamp.from(Instant.parse(snapshotTime));
        return dsl.selectDistinct(SYSTEM_LOAD.SLICE)
                .from(SYSTEM_LOAD)
                .where(SYSTEM_LOAD.SNAPSHOT_TIME.eq(t))
                .fetch(SYSTEM_LOAD.SLICE)
                .stream()
                .map(Long::parseLong)
                .collect(Collectors.toSet());
    }

    /**
     * Create an in-process group service for tests, with an implementation of
     * {@link GroupServiceBlockingStub#getGroups(GetGroupsRequest)} that returns a fixed
     * group structure for these tests.
     *
     * @return group definitions for tests
     * @throws IOException if there's an problem
     */
    private GroupServiceBlockingStub createGroupService() throws IOException {
        GroupServiceImplBase serviceImpl = new GroupServiceImplBase() {
            @Override
            public void getGroups(GetGroupsRequest req, StreamObserver<Grouping> resp) {
                resp.onNext(CLUSTER1);
                resp.onNext(CLUSTER2);
                resp.onCompleted();
            }
        };
        grpcCleanup.register(InProcessServerBuilder.forName("mytest")
                .directExecutor().addService(serviceImpl).build().start());
        ManagedChannel chan = grpcCleanup.register(
                InProcessChannelBuilder.forName("mytest").directExecutor().build());
        return GroupServiceGrpc.newBlockingStub(chan);
    }


    /**
     * Create a {@link Grouping} structure represeting a cluster.
     *
     * @param clusterId id of cluster group
     * @param memberIds ids of member PM entities
     * @return the Grouping structure
     */
    private static Grouping createCluster(long clusterId, Long... memberIds) {
        return Grouping.newBuilder()
                .setId(clusterId)
                .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.COMPUTE_HOST_CLUSTER)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .addAllMembers(Arrays.asList(memberIds))
                                        .build())
                                .build())
                        .build())
                .build();
    }


    /**
     * Create a {@link TopologyEntityDTO} for a PM selling all system-load host commodities with
     * capacities calculated by multiplying a base value by the position of the commodity in the
     * {@link SystemLoadCommodity} members list.
     *
     * @param id           entity id
     * @param capacityBase base capacity value
     * @return the new PM entity
     */
    private static TopologyEntityDTO createPm(long id, double capacityBase) {
        return TopologyEntityDTO.newBuilder()
                .setOid(id)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addAllCommoditySoldList(hostCommodities.stream()
                        .map(type -> CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(type.getSdkType().getNumber())
                                        .build())
                                .setCapacity(capacityBase * (type.ordinal() + 1))
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * Create a {@link TopologyEntityDTO} for a STORAGE selling all system-load storage
     * commodities, with capacities calculated by multiplying a base value by the position of
     * the commodity in the {@link SystemLoadCommodity} members list.
     *
     * @param id           entity id
     * @param capacityBase base capacity value
     * @param buyers       PMs to which this storage sells DSPM access
     * @return the new STORAGE entity
     */
    private static TopologyEntityDTO createStorage(long id, double capacityBase, TopologyEntityDTO... buyers) {
        return TopologyEntityDTO.newBuilder()
                .setOid(id)
                .setEntityType(EntityType.STORAGE_VALUE)
                .addAllCommoditySoldList(storageCommodities.stream()
                        .map(type -> CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(type.getSdkType().getNumber())
                                        .build())
                                .setCapacity(capacityBase * (type.ordinal() + 1))
                                .build())
                        .collect(Collectors.toList()))
                .addAllCommoditySoldList(Arrays.stream(buyers)
                        .map(buyer -> CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                                        .build())
                                .setAccesses(buyer.getOid())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * Create a {@link TopologyEntityDTO} for a VM selling and buying all system-load commodities,
     * with capacity, used and peak values that are all calculated by multiplying corresponding
     * base values by the position of the commodity in the {@link SystemLoadCommodity} members
     * list.
     *
     * @param id           entity id
     * @param baseUsage    base for used values
     * @param basePeak     base for peak values
     * @param baseCapacity base for capacity values
     * @param sellers      PMs that sell to this VM
     * @return the new VM entity
     */
    private static TopologyEntityDTO createVm(
            long id, double baseUsage, double basePeak, double baseCapacity, TopologyEntityDTO... sellers) {
        return TopologyEntityDTO.newBuilder()
                .setOid(id)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addAllCommoditySoldList(Arrays.stream(SystemLoadCommodity.values())
                        .map(type -> CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(type.getSdkType().getNumber())
                                        .build())
                                .setCapacity(baseCapacity * (type.ordinal() + 1))
                                .setPeak(basePeak * (type.ordinal() + 1))
                                .setUsed(baseUsage * (type.ordinal() + 1))
                                .build())
                        .collect(Collectors.toList()))
                .addAllCommoditiesBoughtFromProviders(Arrays.stream(sellers)
                        .map(seller -> CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(seller.getOid())
                                .setProviderEntityType(seller.getEntityType())
                                .addAllCommodityBought(Arrays.stream(SystemLoadCommodity.values())
                                        .map(type -> CommodityBoughtDTO.newBuilder()
                                                .setCommodityType(CommodityType.newBuilder()
                                                        .setType(type.getSdkType().getNumber())
                                                        .build())
                                                .setUsed(baseUsage * (type.ordinal() + 1))
                                                .setPeak(basePeak * (type.ordinal() + 1))
                                                .build())
                                        .collect(Collectors.toList()))
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * Given a {@link TopologyEntityDTO}, create a copy that also buys and sells every non-system-load
     * commodity.
     *
     * <p>All capacity, used, and peak values for the added commodities are 1.0, and the copy buys
     * each added commodity from every PM that is a seller to the template VM.</p>
     *
     * @param template template VM
     * @return augmented copy
     */
    private TopologyEntityDTO getExtendedEntity(TopologyEntityDTO template) {
        final TopologyEntityDTO.Builder copy = TopologyEntityDTO.newBuilder().mergeFrom(template);
        final CommoditiesBoughtFromProvider.Builder cbfp =
                template.getCommoditiesBoughtFromProvidersCount() == 0
                        ? CommoditiesBoughtFromProvider.newBuilder()
                        : CommoditiesBoughtFromProvider.newBuilder().mergeFrom(
                        template.getCommoditiesBoughtFromProviders(0));
        Arrays.stream(CommodityDTO.CommodityType.values())
                .filter(type -> !SystemLoadCommodity.fromSdkCommodityType(type.getNumber()).isPresent())
                .forEach(type -> {
                    cbfp.addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(type.getNumber()).build())
                            .setUsed(1.0).setPeak(1.0).build());
                    copy.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder().setType(type.getNumber()).build())
                            .setCapacity(1.0).build());
                });
        return copy.build();
    }

    /**
     * Check that the correct records were written to the transient table for a given VM appearing in
     * the topology, for the given timestamp and cluster id.
     *
     * @param time         topolgoy timestamp, in format to be parsed by {@link Instant#parse(CharSequence)}
     * @param clusterId    id of cluster to check
     * @param entityId     entity id of VM
     * @param baseUsed     base for used values
     * @param basePeak     base for peak values
     * @param baseCapacity base for capacity values
     */
    private void checkEntityCommodityRecords(String time, long clusterId, long entityId,
            double baseUsed, double basePeak, double baseCapacity) {
        // get the transient table - it's the only table we wrote to other than SYSTEM_LOAD
        final Optional<String> transTableName = dsl.fetchValues("SHOW TABLES").stream()
                .map(value -> (String)value)
                .filter(t -> t.startsWith(SYSTEM_LOAD.getName()))
                .filter(t -> !t.equals(SYSTEM_LOAD.getName()))
                .findFirst();
        if (transTableName.isPresent()) {
            final Table<Record> transTable = DSL.table(transTableName.get());
            final List<SystemLoadRecord> recordsStream = dsl.selectFrom(transTable).stream()
                    .map(r -> r.into(SystemLoadRecord.class))
                    .filter(r -> r.getSnapshotTime().equals(Timestamp.from(Instant.parse(time))))
                    .filter(r -> r.getSlice().equals(Long.toString(clusterId)))
                    .filter(r -> r.getUuid().equals(Long.toString(entityId)))
                    .collect(Collectors.toList());
            checkBoughtCommodities(recordsStream, baseUsed, basePeak);
            checkSoldCommodities(recordsStream, baseUsed, basePeak, baseCapacity);
        } else {
            fail("Unable to locate transient system-load table");
        }

    }

    /**
     * Check that the given list of bought commodities records has the correct used and peak values
     * given the base values from which they were caluculated.
     *
     * @param records  bought commodities records for some VM
     * @param baseUsed base for used values
     * @param basePeak base vor peak used values
     */
    private void checkBoughtCommodities(List<SystemLoadRecord> records, double baseUsed, double basePeak) {
        records.stream().filter(r -> r.getRelation() == RelationType.COMMODITIESBOUGHT).forEach(r -> {
            final SystemLoadCommodity type = SystemLoadCommodity.valueOf(r.getPropertyType());
            assertThat(r.getPropertySubtype(), is(StringConstants.USED));
            assertThat(r.getAvgValue(), is(baseUsed * (type.ordinal() + 1)));
            assertThat(r.getMinValue(), is(baseUsed * (type.ordinal() + 1)));
            assertThat(r.getMaxValue(), is(basePeak * (type.ordinal() + 1)));
            assertNull(r.getCapacity());
        });
    }

    /**
     * Check that the given sold commodities records have the correct capacity values, given the
     * base capacity from which they were calculated.
     *
     * @param records      sold capacity records for some VM
     * @param baseUsed     base for used value
     * @param basePeak     base for peak value
     * @param baseCapacity base for capacity values
     *
     */
    private void checkSoldCommodities(List<SystemLoadRecord> records, double baseUsed,
                                      double basePeak, double baseCapacity) {
        records.stream().filter(r -> r.getRelation() == RelationType.COMMODITIES).forEach(r -> {
            final SystemLoadCommodity type = SystemLoadCommodity.valueOf(r.getPropertyType());
            assertThat(r.getPropertySubtype(), is(StringConstants.USED));
            assertThat(r.getAvgValue(), is(baseUsed * (type.ordinal() + 1)));
            assertThat(r.getMinValue(), is(baseUsed * (type.ordinal() + 1)));
            assertThat(r.getMaxValue(), is(basePeak * (type.ordinal() + 1)));
            assertThat(r.getCapacity(), is(baseCapacity * (type.ordinal() + 1)));
        });
    }

    /**
     * Check that the correct utilization records were written for the given cluster.
     *
     * <p>Base values supplied as arguments should be the sum of the base values used in
     * constructing the relevant entities appearing in the procesed topology.</p>
     *
     * @param time             timestamp of topology
     * @param clusterId        id of cluster to check
     * @param hostCapacityBase aggregate base capacity for PM entities
     * @param stgCapacityBase  aggregate base capacity for STORAGE entities
     * @param hostUsedBase     aggregate base used value for PM commodity buys
     * @param stgUsedBase      aggregate base peak value for STORAGE commodity buys
     * @param systemLoad       expected overall system load for cluster
     */
    private void checkUtilizationRecords(String time, long clusterId,
            double hostCapacityBase, double stgCapacityBase, double hostUsedBase, double stgUsedBase,
            double systemLoad) {
        List<SystemLoadRecord> utilizationRecords = getUtilizationRecords(time, clusterId);
        // we should have a record for every system-load commodity
        assertThat(utilizationRecords.size(), is(SystemLoadCommodity.values().length));
        // make sure that all slice capacity and used values are correct for each commodity
        for (final SystemLoadRecord rec : utilizationRecords) {
            final SystemLoadCommodity type = SystemLoadCommodity.valueOf(rec.getPropertySubtype());
            double base = hostCommodities.contains(type) ? hostCapacityBase : stgCapacityBase;
            assertThat("Incorrect slice capacity for " + type, rec.getCapacity(), is(base * (type.ordinal() + 1)));
            base = hostCommodities.contains(type) ? hostUsedBase : stgUsedBase;
            assertThat("Incorrect slice avg used for " + type, rec.getAvgValue(), is(base * (type.ordinal() + 1)));
            assertThat("Incorrect slice min used for " + type, rec.getMinValue(), is(base * (type.ordinal() + 1)));
            assertThat("Incorrect slice max used for " + type, rec.getMaxValue(), is(base * (type.ordinal() + 1)));
        }
        // make sure we have the correct overall system load value
        assertThat("Incorrect system load in cluster " + clusterId, getSystemLoad(time, clusterId), is(systemLoad));
    }

    /**
     * Create a topology chunk containing the given entities.
     *
     * @param entities entities to appear in the topology
     * @return a chunk contining the entities
     */
    private List<TopologyEntityDTO> createChunk(TopologyEntityDTO... entities) {
        return Arrays.asList(entities);
    }

    /**
     * Create a {@link TopologyInfo} with a given creation time.
     *
     * @param time creation timestamp (to be parsed by {@link Instant#parse(CharSequence)})
     * @return topology info
     */
    private TopologyInfo createTopoInfo(String time) {
        return TopologyInfo.newBuilder()
                .setCreationTime(Instant.parse(time).toEpochMilli())
                .build();
    }

    /**
     * Retrieve utilization records written to the mock db for a given snapshot time and cluster.
     *
     * <p>These records will have been written to the main SYSTEM_LOAD table, and will have
     * "system_load" property type, but not "system load" as subtype.</p>
     *
     * @param time      snapshot time
     * @param clusterId cluster id
     * @return utilization records
     */
    private List<SystemLoadRecord> getUtilizationRecords(String time, long clusterId) {
        Timestamp timestamp = Timestamp.from(Instant.parse(time));
        return dsl.selectFrom(SYSTEM_LOAD).stream()
                .filter(r -> r.getSnapshotTime().equals(timestamp))
                .filter(r -> r.getSlice().equals(Long.toString(clusterId)))
                .filter(r -> r.getPropertyType().equals(StringConstants.SYSTEM_LOAD))
                .filter(r -> !r.getPropertySubtype().equals(StringConstants.SYSTEM_LOAD))
                .collect(Collectors.toList());
    }

    /**
     * Retrieve the overall system load recorded in the mock db for the given snapshot time and
     * cluster.
     *
     * <p>The system load value is written to the main SYSTEM_LOAD table with property type
     * and subtype both set to "system_load"</p>
     *
     * @param time      snapshot time
     * @param clusterId cluster id
     * @return the overall system load value
     */
    private Double getSystemLoad(String time, long clusterId) {
        SystemLoadRecord record = dsl.selectFrom(SYSTEM_LOAD)
                .where(SYSTEM_LOAD.SLICE.eq(Long.toString(clusterId)))
                .and(SYSTEM_LOAD.SNAPSHOT_TIME.eq(Timestamp.from(Instant.parse(time))))
                .and(SYSTEM_LOAD.PROPERTY_TYPE.eq(StringConstants.SYSTEM_LOAD))
                .and(SYSTEM_LOAD.PROPERTY_SUBTYPE.eq(StringConstants.SYSTEM_LOAD))
                .and(SYSTEM_LOAD.RELATION.eq(RelationType.COMMODITIES)).fetchOne();
        return record != null ? record.getAvgValue() : null;
    }
}
