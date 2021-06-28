package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.util.RecordTestUtil.captureSink;
import static com.vmturbo.extractor.util.TopologyTestUtil.mkEntity;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.CloudCostStatsAvailable;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions.EntityCost;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.util.TopologyTestUtil;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Tests for EntityCostWriter class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
public class EntityCostListenerTest {

    @Autowired
    private ExtractorDbConfig dbConfig;

    private EntityCostListener listener;
    final TopologyInfo info = TopologyTestUtil.mkRealtimeTopologyInfo(1L);
    final Timestamp snapshotTime = new Timestamp(info.getCreationTime());
    final MultiStageTimer timer = mock(MultiStageTimer.class);
    private final DataProvider dataProvider = mock(DataProvider.class);
    private final BottomUpCostData bottomUpCostData = spy(new BottomUpCostData(1L));
    private List<Record> entityCostRecordsCapture;
    private WriterConfig writerConfig = mock(WriterConfig.class);
    private final CostServiceMole costService = spy(CostServiceMole.class);
    private static final long realtimeTopologyContextId = 77777;

    /**
     * Mock tests for gRPC services.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(costService);


    /**
     * Set up for tests.
     *
     * <p>We create a mock DSLContext that won't do anything, and we also set up record-capturing
     * record sinks and arrange for our test listener instance to use them, so we can verify that
     * the records written by the instance are correct.</p>
     *
     * @throws UnsupportedDialectException if our db endpoint is misconfigured
     * @throws SQLException                if there's a DB issue
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        setupWriterAndSinks();
    }

    private void setupWriterAndSinks()
            throws UnsupportedDialectException, InterruptedException, SQLException {
        final DbEndpoint endpoint = spy(dbConfig.ingesterEndpoint());
        doReturn(mock(DSLContext.class)).when(endpoint).dslContext();
        DslRecordSink entityCostInserterSink = mock(DslRecordSink.class);
        this.entityCostRecordsCapture = captureSink(entityCostInserterSink, false);
        final DataPack<Long> oidPack = new LongDataPack();
        this.listener = spy(new EntityCostListener(dataProvider, endpoint,
                Executors.newSingleThreadScheduledExecutor(), writerConfig, true, realtimeTopologyContextId));
        doReturn(entityCostInserterSink).when(listener).getEntityCostInserterSink();
        doReturn(bottomUpCostData).when(dataProvider).getBottomUpCostData();
    }

    /**
     * Test that the overall listener flow in the {@link EntityCostListener} proceeds as expected
     * and results in the correct number of records reported as ingested.
     *
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the db endpoint is misconfigured
     * @throws IOException                 if there's an IO related issue
     */
    @Test
    public void testIngesterFlow()
            throws InterruptedException, SQLException, UnsupportedDialectException, IOException {
        listener.onCostNotificationReceived(CostNotification.newBuilder()
                .setCloudCostStatsAvailable(CloudCostStatsAvailable.newBuilder()
                        .setSnapshotDate(1L).build())
                .build());
        // We didn't have costs for either of our entities
        assertThat(entityCostRecordsCapture, is(empty()));
    }

    /**
     * Test that cost data is propertly written to the database.
     *
     * @throws InterruptedException        if interrupted
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the DB endpoint is misconfigured
     * @throws IOException                 if there's some other issue
     */
    @Test
    public void testCostDataIsCorrectlyPersisted()
            throws InterruptedException, SQLException, UnsupportedDialectException, IOException {

        final TopologyEntityDTO vm1 = mkEntity(VIRTUAL_MACHINE);
        final TopologyEntityDTO vm2 = mkEntity(VIRTUAL_MACHINE);
        final Optional<ImmutableList<StatRecord>> vm1Costs = Optional.of(ImmutableList.of(
                createStatRecord(vm1.getOid(), 1L,
                        Cost.CostCategory.ON_DEMAND_COMPUTE, Cost.CostSource.ON_DEMAND_RATE, 1),
                createStatRecord(vm1.getOid(), 1L,
                        Cost.CostCategory.ON_DEMAND_COMPUTE, Cost.CostSource.ENTITY_UPTIME_DISCOUNT, -0.3f),
                createStatRecord(vm1.getOid(), 1L,
                        Cost.CostCategory.ON_DEMAND_LICENSE, Cost.CostSource.ON_DEMAND_RATE, 0.1f)
        ));
        final Optional<ImmutableList<StatRecord>> vm2Costs = Optional.of(ImmutableList.of(
                createStatRecord(vm2.getOid(), 1L,
                        Cost.CostCategory.ON_DEMAND_COMPUTE, Cost.CostSource.ON_DEMAND_RATE, 2)
        ));
        doAnswer(i -> {
            long oid = i.getArgumentAt(0, Long.class);
            if (oid == vm1.getOid()) {
                return vm1Costs;
            } else if (oid == vm2.getOid()) {
                return vm2Costs;
            } else {
                return Optional.empty();
            }
        }).when(bottomUpCostData).getEntityCosts(anyLong());
        doReturn(Arrays.stream(new long[]{vm1.getOid(), vm2.getOid(), Long.MAX_VALUE}))
                .when(bottomUpCostData).getEntityOids();

        listener.onCostNotificationReceived(CostNotification.newBuilder()
                .setCloudCostStatsAvailable(CloudCostStatsAvailable.newBuilder()
                        .setSnapshotDate(1L).build())
                .build());
        // we had cost data for the 2 vms
        // We didn't have costs for either of our entities
        assertThat(entityCostRecordsCapture.size(), is(9));
        assertThat(entityCostRecordsCapture, containsInAnyOrder(
                getRecord(snapshotTime, vm1.getOid(), CostCategory.ON_DEMAND_COMPUTE, CostSource.ON_DEMAND_RATE, 1),
                getRecord(snapshotTime, vm1.getOid(), CostCategory.ON_DEMAND_COMPUTE, CostSource.ENTITY_UPTIME_DISCOUNT, -0.3f),
                getRecord(snapshotTime, vm1.getOid(), CostCategory.ON_DEMAND_COMPUTE, CostSource.TOTAL, 0.7f),
                getRecord(snapshotTime, vm1.getOid(), CostCategory.ON_DEMAND_LICENSE, CostSource.ON_DEMAND_RATE, 0.1f),
                getRecord(snapshotTime, vm1.getOid(), CostCategory.ON_DEMAND_LICENSE, CostSource.TOTAL, 0.1f),
                getRecord(snapshotTime, vm1.getOid(), CostCategory.TOTAL, CostSource.TOTAL, 0.8f),
                getRecord(snapshotTime, vm2.getOid(), CostCategory.ON_DEMAND_COMPUTE, CostSource.ON_DEMAND_RATE, 2),
                getRecord(snapshotTime, vm2.getOid(), CostCategory.ON_DEMAND_COMPUTE, CostSource.TOTAL, 2),
                getRecord(snapshotTime, vm2.getOid(), CostCategory.TOTAL, CostSource.TOTAL, 2)
        ));

    }

    /**
     * Test that plan cost notification is ignored.
     */
    @Test
    public void testPlanNotificationIsIgnored() {
        listener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setTopologyContextId(1234)
                        .setType(StatusUpdateType.PLAN_ENTITY_COST_UPDATE))
                .build());
        verifyZeroInteractions(dataProvider);
    }

    /**
     * Test that real time projected cost is processed.
     */
    @Test
    public void testRealtimeProjectedCosts() {
        listener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId)
                        .setType(StatusUpdateType.PROJECTED_COST_UPDATE))
                .build());
        verify(dataProvider).fetchProjectedBottomUpCostData(any());
    }

    private Record getRecord(final Timestamp time, final long oid,
            final CostCategory category, final CostSource source, final float cost) {
        Record r = new Record(EntityCost.TABLE);
        r.set(EntityCost.TIME, time);
        r.set(EntityCost.ENTITY_OID, oid);
        r.set(EntityCost.CATEGORY, category);
        r.set(EntityCost.SOURCE, source);
        r.set(EntityCost.COST, cost);
        return r;
    }

    private static StatRecord createStatRecord(
            long oid, long snapshotTime,
            Cost.CostCategory category, Cost.CostSource source, float cost) {
        return StatRecord.newBuilder()
                .setAssociatedEntityId(oid)
                .setCategory(category)
                .setCostSource(source)
                .setValues(StatValue.newBuilder()
                        .setAvg(cost)
                        .build())
                .build();
    }
}
