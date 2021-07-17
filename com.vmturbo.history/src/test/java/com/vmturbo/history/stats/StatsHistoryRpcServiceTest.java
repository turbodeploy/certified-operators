 package com.vmturbo.history.stats;

 import static com.vmturbo.common.protobuf.utils.StringConstants.USED;
 import static com.vmturbo.history.schema.RelationType.COMMODITIES;
 import static com.vmturbo.history.stats.StatsTestUtils.newStatRecord;
 import static junit.framework.TestCase.assertTrue;
 import static org.hamcrest.CoreMatchers.equalTo;
 import static org.hamcrest.CoreMatchers.is;
 import static org.hamcrest.Matchers.contains;
 import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
 import static org.junit.Assert.assertEquals;
 import static org.junit.Assert.assertFalse;
 import static org.junit.Assert.assertNotNull;
 import static org.junit.Assert.assertThat;
 import static org.junit.Assert.fail;
 import static org.mockito.Matchers.any;
 import static org.mockito.Matchers.anyLong;
 import static org.mockito.Matchers.anyObject;
 import static org.mockito.Matchers.eq;
 import static org.mockito.Mockito.atLeastOnce;
 import static org.mockito.Mockito.doReturn;
 import static org.mockito.Mockito.doThrow;
 import static org.mockito.Mockito.mock;
 import static org.mockito.Mockito.times;
 import static org.mockito.Mockito.verify;
 import static org.mockito.Mockito.verifyNoMoreInteractions;
 import static org.mockito.Mockito.when;

 import java.sql.Date;
 import java.sql.Timestamp;
 import java.time.Duration;
 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.Collections;
 import java.util.Iterator;
 import java.util.List;
 import java.util.Map;
 import java.util.Optional;
 import java.util.Set;
 import java.util.concurrent.ExecutorService;
 import java.util.concurrent.TimeUnit;
 import java.util.stream.Collectors;
 import java.util.stream.Stream;

 import javax.annotation.Nonnull;

 import com.google.common.collect.ImmutableList;
 import com.google.common.collect.ImmutableMap;
 import com.google.common.collect.ImmutableSet;
 import com.google.common.collect.Lists;
 import com.google.common.collect.Sets;

 import io.grpc.Status.Code;
 import io.grpc.StatusRuntimeException;
 import io.grpc.stub.StreamObserver;

 import org.jooq.Record;
 import org.jooq.Record3;
 import org.junit.Assert;
 import org.junit.Before;
 import org.junit.Rule;
 import org.junit.Test;
 import org.mockito.ArgumentCaptor;
 import org.mockito.Mockito;

 import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
 import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
 import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
 import com.vmturbo.common.protobuf.stats.Stats;
 import com.vmturbo.common.protobuf.stats.Stats.ClusterHeadroomInfo;
 import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequestForHeadroomPlan;
 import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
 import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
 import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
 import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityGroup;
 import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityGroupList;
 import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
 import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingRequest;
 import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingResponse;
 import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
 import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.GetStatsDataRetentionSettingsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryRequest;
 import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryResponse;
 import com.vmturbo.common.protobuf.stats.Stats.GetVolumeAttachmentHistoryResponse.VolumeAttachmentHistory;
 import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
 import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
 import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
 import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
 import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
 import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
 import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingRequest;
 import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingRequest;
 import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingResponse;
 import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
 import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
 import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
 import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
 import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
 import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
 import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
 import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
 import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
 import com.vmturbo.common.protobuf.utils.StringConstants;
 import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
 import com.vmturbo.components.api.test.GrpcTestServer;
 import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
 import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
 import com.vmturbo.components.common.setting.SettingDTOUtil;
 import com.vmturbo.history.db.EntityType;
 import com.vmturbo.history.db.HistorydbIO;
 import com.vmturbo.history.db.VmtDbException;
 import com.vmturbo.history.db.bulk.BulkLoader;
 import com.vmturbo.history.ingesters.live.writers.SystemLoadWriter;
 import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
 import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
 import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
 import com.vmturbo.history.stats.ClusterStatsReader.ClusterStatsRecordReader;
 import com.vmturbo.history.stats.live.SystemLoadReader;
 import com.vmturbo.history.stats.projected.ProjectedStatsStore;
 import com.vmturbo.history.stats.readers.LiveStatsReader;
 import com.vmturbo.history.stats.readers.LiveStatsReader.StatRecordPage;
 import com.vmturbo.history.stats.readers.VolumeAttachmentHistoryReader;
 import com.vmturbo.history.stats.snapshots.StatSnapshotCreator;
 import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Test gRPC methods to handle snapshot requests.
 */
public class StatsHistoryRpcServiceTest {

    private static final long PLAN_UUID = 1L;
    private static final long ENTITY_UUID = 2L;
    private static final long PLAN_OID = PLAN_UUID;
    private static final Timestamp SNAPSHOT_TIME = new Timestamp(123L);
    private static final long REALTIME_CONTEXT_ID = 7L;
    private static final String NUM_VMS = PropertySubType.NumVms.getApiParameterName();
    private static final String HEADROOM_VMS = PropertySubType.HeadroomVms.getApiParameterName();
    private static final String UTILIZATION = PropertySubType.Utilization.getApiParameterName();
    private final long topologyContextId = 8L;

    private LiveStatsReader mockLivestatsreader = mock(LiveStatsReader.class);

    private PlanStatsReader mockPlanStatsReader = mock(PlanStatsReader.class);

    private ClusterStatsReader mockClusterStatsReader = mock(ClusterStatsReader.class);

    private BulkLoader<ClusterStatsByDayRecord> mockLoader = mock(BulkLoader.class);

    private HistorydbIO historyDbio = mock(HistorydbIO.class);

    private ProjectedStatsStore mockProjectedStatsStore = mock(ProjectedStatsStore.class);

    private EntityStatsPaginationParamsFactory paginationParamsFactory =
            mock(EntityStatsPaginationParamsFactory.class);
    private StatRecordBuilder statRecordBuilderSpy =
                    Mockito.spy(StatsConfig.createStatRecordBuilder(mockLivestatsreader));

    private StatSnapshotCreator statSnapshotCreatorSpy =
                    Mockito.spy(StatsConfig.createStatSnapshotCreator(mockLivestatsreader));

    private SystemLoadReader systemLoadReader = mock(SystemLoadReader.class);

    private GetEntityStatsResponseStreamObserver getEntityStatsResponseStreamObserver =
            new GetEntityStatsResponseStreamObserver();


    private RequestBasedReader<GetPercentileCountsRequest, PercentileChunk> percentileReader
            = mock(RequestBasedReader.class);

    private VolumeAttachmentHistoryReader volumeAttachmentHistoryReader =
        mock(VolumeAttachmentHistoryReader.class);

    private StatsHistoryRpcService statsHistoryRpcService =
            Mockito.spy(new StatsHistoryRpcService(REALTIME_CONTEXT_ID,
                    mockLivestatsreader, mockPlanStatsReader,
                    mockClusterStatsReader, mockLoader,
                    historyDbio, mockProjectedStatsStore,
                    paginationParamsFactory,
                    statSnapshotCreatorSpy,
                    statRecordBuilderSpy,
                    systemLoadReader, 100,
                    percentileReader,
                    volumeAttachmentHistoryReader));

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryRpcService);

    private StatsHistoryServiceBlockingStub clientStub;

    @Before
    public void setup() {
        clientStub = StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
    }

    /**
     * Test the generated count statistics.
     */
    @Test
    public void testGetStatsCounts() throws Exception {
        // Arrange
        List<Long> entities = Arrays.asList(1L, 2L, 3L);

        // convert to the standard time format we return
        // the two values for "c1" will be averaged"
        final float c1Value1 = 123;
        final String propType = "c1";
        final float c1Value2 = 456;
        final float c1Avg = (c1Value1 + c1Value2) / 2;
        // only one value for "c2"
        final float c2Value = 789;
        final String propType2 = "c2";
        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecord(SNAPSHOT_TIME, c1Value1, propType, USED),
            newStatRecord(SNAPSHOT_TIME, c1Value2, propType, USED),
            newStatRecord(SNAPSHOT_TIME, c2Value, propType2, USED));

        when(mockLivestatsreader.getRecords(anyObject(), anyObject(), anyObject()))
                .thenReturn(statsRecordsList);
        Stats.GetAveragedEntityStatsRequest.Builder testStatsRequest =
                Stats.GetAveragedEntityStatsRequest.newBuilder();
        testStatsRequest.addAllEntities(entities);

        // Act
        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getAveragedEntityStats(testStatsRequest.build()).forEachRemaining(snapshots::add);


        // Assert
        assertThat(snapshots.size(), is(1));
        final StatSnapshot snapshot = snapshots.get(0);
        assertThat(snapshot.getSnapshotDate(), is(SNAPSHOT_TIME.getTime()));
        assertThat(snapshot.getStatRecordsCount(), is(2));

        // The order is not guaranteed as map iteration is used in the implementation.
        StatRecord statRecord = snapshot.getStatRecords(0);
        StatRecord statRecord2 = snapshot.getStatRecords(1);

        if (propType.equals(statRecord.getName())) {
            // statRecord is for c1 and statRecord2 is for c2
            checkStatRecord(propType, c1Avg, Math.min(c1Value1, c1Value2), Math.max(c1Value1, c1Value2), statRecord);
            checkStatRecord(propType2, c2Value, c2Value, c2Value, statRecord2);
        } else if (propType.equals(statRecord2.getName())) {
            // statRecord is for c2 and statRecord2 is for c1
            checkStatRecord(propType2, c2Value, c2Value, c2Value, statRecord);
            checkStatRecord(propType, c1Avg, Math.min(c1Value1, c1Value2), Math.max(c1Value1, c1Value2), statRecord2);
        } else {
            fail("Wrong stat records: " + snapshot.getStatRecordsList());
        }
    }

    /**
     * Check the values for a stat record - property type, avg, min, and max.
     *
     * @param propType the property type string
     * @param c1Value the value to test
     * @param c1Min the min value to test
     * @param c1Max the max value to test
     * @param statRecord the record to check
     */
    private void checkStatRecord(String propType, float c1Value, float c1Min, float c1Max,
                                 StatRecord statRecord) {
        assertThat(statRecord.getName(), is(propType));
        assertThat(statRecord.getValues().getAvg(), is(c1Value));
        assertThat(statRecord.getValues().getMin(), is(c1Min / 2));
        assertThat(statRecord.getValues().getMax(), is(c1Max * 2));
    }

    /**
     * Test stats request with uuid == "Market" -> live market stats query.
     *
     * @throws Exception not expected
     */
    @Test
    public void testMarketStats() throws Exception {
        // arrange
        long startDate = System.currentTimeMillis();
        long endDate = startDate + Duration.ofSeconds(1).toMillis();
        List<CommodityRequest> commodityRequests = buildCommodityRequests("c1", "c2", "c3");
        StatsFilter.Builder reqStatsBuilder = StatsFilter.newBuilder()
                .setStartDate(startDate)
                .setEndDate(endDate)
                .addAllCommodityRequests(commodityRequests);

        // full 'Market' stats request has no entities
        Stats.GetAveragedEntityStatsRequest testStatsRequest = Stats.GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(reqStatsBuilder)
                .build();

        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecord(SNAPSHOT_TIME, 1, "c1", "c1-subtype"),
            newStatRecord(SNAPSHOT_TIME, 2, "c2", "c2-subtype"),
            newStatRecord(SNAPSHOT_TIME, 3, "c3", "c3-subtype"));
        when(mockLivestatsreader.getFullMarketStatsRecords(reqStatsBuilder.build(),
            GlobalFilter.getDefaultInstance()))
                .thenReturn(statsRecordsList);

        // act
        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getAveragedEntityStats(testStatsRequest).forEachRemaining(snapshots::add);

        // assert
        assertThat(snapshots.size(), is(1));
        verify(mockLivestatsreader).getFullMarketStatsRecords(eq(reqStatsBuilder.build()), anyObject());
        verifyNoMoreInteractions(mockPlanStatsReader);

    }

    private List<CommodityRequest> buildCommodityRequests(String... commodityNames) {
        return Arrays.stream(commodityNames).map(commodityName -> CommodityRequest.newBuilder()
                .setCommodityName(commodityName)
                .build())
                .collect(Collectors.toList());
    }

    /**
     * Test stats request where the UUID is not the special "Market" uuid, but
     * is known as a valid scenario, i.e. a plan ID.
     *
     * @throws Exception not expected
     */
    @Test
    public void testPlanStats() throws Exception {
        // arrange
        when(historyDbio.entityIdIsPlan(PLAN_UUID)).thenReturn(true);
        ScenariosRecord scenariosRecord = new ScenariosRecord();
        scenariosRecord.setCreateTime(new Timestamp(0));
        when(historyDbio.getScenariosRecord(PLAN_OID)).thenReturn(Optional.of(scenariosRecord));

        long startDate = System.currentTimeMillis();
        long endDate = startDate + Duration.ofSeconds(1).toMillis();
        StatsFilter requestedStats = StatsFilter.newBuilder()
                .setStartDate(startDate)
                .setEndDate(endDate)
                .build();

        // act
        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getAveragedEntityStats(GetAveragedEntityStatsRequest.newBuilder()
            .addEntities(PLAN_UUID)
            .setFilter(requestedStats)
            .build()).forEachRemaining(snapshots::add);

        // assert
        assertThat(snapshots.size(), is(2));
        verify(mockPlanStatsReader).getStatsRecords(eq(PLAN_OID), anyObject(), anyObject());
        verifyNoMoreInteractions(mockLivestatsreader);
    }

    /**
     * Test stats request when the UUID is neither the well known UUID "Market"
     * nor a known scenario, i.e. to be treated as a service entity. The startTime and endTime
     * are specified.
     */
    @Test
    public void testEntityStatsWithTimeRange() throws Exception {
        // arrange
        when(historyDbio.entityIdIsPlan(ENTITY_UUID)).thenReturn(false);
        ScenariosRecord scenariosRecord = new ScenariosRecord();
        when(historyDbio.getScenariosRecord(PLAN_OID)).thenReturn(Optional.of(scenariosRecord));

        long startDate = System.currentTimeMillis();
        long endDate = startDate + Duration.ofSeconds(1).toMillis();
        final List<Long> entityUuids = Lists.newArrayList(ENTITY_UUID);
        final Set<String> entityUuidsStr = Collections.singleton(Long.toString(ENTITY_UUID));

        StatsFilter.Builder reqStatsBuilder = StatsFilter.newBuilder()
            .setStartDate(startDate)
            .setEndDate(endDate);
        final List<CommodityRequest> commodityRequests = buildCommodityRequests("c1", "c2", "c3");
        reqStatsBuilder.addAllCommodityRequests(commodityRequests);

        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecord(SNAPSHOT_TIME, 1, "c1", "c1-subtype"),
            newStatRecord(SNAPSHOT_TIME, 2, "c2", "c2-subtype"),
            newStatRecord(SNAPSHOT_TIME, 3, "c3", "c3-subtype"));
        when(mockLivestatsreader.getRecords(eq(entityUuidsStr), eq(reqStatsBuilder.build()), eq(Collections.emptyList())))
            .thenReturn(statsRecordsList);

        Stats.GetAveragedEntityStatsRequest testStatsRequest = Stats.GetAveragedEntityStatsRequest.newBuilder()
                .addAllEntities(entityUuids)
                .setFilter(reqStatsBuilder)
                .build();
        // act
        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getAveragedEntityStats(testStatsRequest).forEachRemaining(snapshots::add);

        // assert
        assertThat(snapshots.size(), is(1));
        verify(mockLivestatsreader).getRecords(eq(entityUuidsStr), eq(reqStatsBuilder.build()), eq(Collections.emptyList()));
        verifyNoMoreInteractions(mockPlanStatsReader);
    }

    /**
     * Test the min, max, avg, capacity calculations over a number of DB Stats Rows.
     * The test data has 3 rows for the same stat type/subtype, with value 1, 2, 3 respectively.
     *
     * @throws Exception if there's a DB exception - should not happen
     */
    @Test
    public void testAveragedStats() throws Exception {
        // arrange
        when(historyDbio.entityIdIsPlan(ENTITY_UUID)).thenReturn(false);
        ScenariosRecord scenariosRecord = new ScenariosRecord();
        when(historyDbio.getScenariosRecord(PLAN_OID)).thenReturn(Optional.of(scenariosRecord));
        when(historyDbio.getClosestTimestampBefore(any(), any(), any()))
            .thenReturn(Optional.of(new Timestamp(123L)));

        long startDate = System.currentTimeMillis();
        long endDate = startDate + Duration.ofSeconds(1).toMillis();
        final List<Long> queryEntityUuids = Lists.newArrayList(ENTITY_UUID);
        final Set<String> queryEntityUuidsStr = queryEntityUuids.stream()
                .map(oid -> Long.toString(oid))
                .collect(Collectors.toSet());

        StatsFilter.Builder reqStatsBuilder = StatsFilter.newBuilder()
                .setStartDate(startDate)
                .setEndDate(endDate);
        final List<CommodityRequest> commodityRequests = buildCommodityRequests("c1");
        reqStatsBuilder.addAllCommodityRequests(commodityRequests);
        Stats.GetAveragedEntityStatsRequest testStatsRequest = Stats.GetAveragedEntityStatsRequest.newBuilder()
                .addAllEntities(queryEntityUuids)
                .setFilter(reqStatsBuilder)
                .build();

        // three rows for 'c1', values 1, 2, 3 respectively
        final List<Record> statsRecordsList = Lists.newArrayList(
            newStatRecord(SNAPSHOT_TIME, 1f, "c1", UTILIZATION),
            newStatRecord(SNAPSHOT_TIME, 2f, "c1", UTILIZATION),
            newStatRecord(SNAPSHOT_TIME, 3f, "c1", UTILIZATION));

        when(mockLivestatsreader.getRecords(eq(queryEntityUuidsStr), eq(reqStatsBuilder.build()), eq(Collections.emptyList())))
                .thenReturn(statsRecordsList);

        // act
        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getAveragedEntityStats(testStatsRequest).forEachRemaining(snapshots::add);

        // assert
        assertThat(snapshots.size(), is(1));
        final StatSnapshot statSnapshot = snapshots.get(0);
        List<StatRecord> snapshotRecords = statSnapshot.getStatRecordsList();
        assertThat(snapshotRecords.size(), equalTo(1));
        final StatRecord statRecord = snapshotRecords.get(0);
        // values are 1, 2, 3;
        //      avgValue = 2.0;
        assertThat(statRecord.getValues().getAvg(), equalTo(2f));
        //      max = 2 x value = 2, 4, 6; Max = 6;
        assertThat(statRecord.getValues().getMax(), equalTo(6f));
        //      min = 0.5 x value = 0.5, 1.0, 1.5; min = 0.5;
        assertThat(statRecord.getValues().getMin(), equalTo(0.5f));
        // in this case current := Max since subtype != type
        assertThat(statRecord.getCurrentValue(), equalTo(6f));
        assertThat(statRecord.getUsed().getTotal(), equalTo(6f));
        //      capacity = sum(3 x value) = 3, 6, 9; total 18
        assertThat(statRecord.getCapacity().getMin(), equalTo(3f));
        assertThat(statRecord.getCapacity().getMax(), equalTo(9f));
        assertThat(statRecord.getCapacity().getAvg(), equalTo(6f));
        assertThat(statRecord.getCapacity().getTotal(), equalTo(18f));
        // reserved should be 0 since we didn't set an effective capacity %.
        assertThat(statRecord.getReserved(), equalTo(0f));
    }


    @Test
    public void testDeletePlanStats() throws VmtDbException {
        clientStub.deletePlanStats(
                createDeletePlanStatsRequest(topologyContextId));
        verify(historyDbio).deletePlanStats(topologyContextId);
    }

    @Test
    public void testDeletePlanStatsMissingParameter() {
        try {
            clientStub.deletePlanStats(createDeletePlanStatsRequest());
        } catch (StatusRuntimeException e) {
            assertThat(e, GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                    .anyDescription());
        }
    }

    @Test
    public void testDeletePlanStatsFailure() throws Exception {
        VmtDbException dbException = new VmtDbException(
            VmtDbException.DELETE_ERR, "Error deleting plan");

        doThrow(dbException).when(historyDbio)
            .deletePlanStats(topologyContextId);

        try {
            clientStub.deletePlanStats(createDeletePlanStatsRequest(topologyContextId));
        } catch (StatusRuntimeException e) {
            assertThat(e, GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
                    .anyDescription());
        }
    }

    @Test
    public void testGetProjectedStats() {
        // arrange
        Set<String> commodityNames = Collections.emptySet();
        final Set<Long> entityOids = Collections.emptySet();
        ProjectedStatsRequest request = ProjectedStatsRequest.newBuilder()
                .addAllEntities(entityOids)
                .addAllCommodityName(commodityNames)
                .build();
        StatSnapshot statSnapshot = StatSnapshot.newBuilder()
                .build();
        when(mockProjectedStatsStore.getStatSnapshotForEntities(entityOids, commodityNames, Collections.emptySet()))
                .thenReturn(Optional.of(statSnapshot));

        // act
        final ProjectedStatsResponse response = clientStub.getProjectedStats(request);

        // assert
        assertTrue(response.hasSnapshot());
        StatSnapshot responseSnapshot = response.getSnapshot();
        assertThat(responseSnapshot, equalTo(statSnapshot));
    }

    /**
     * Request individual stats for 3 entities.
     */
    @Test
    public void testGetProjectedEntityStats() {
        final Set<Long> targetEntities = Sets.newHashSet(1L, 2L);
        final Set<String> targetCommodities = Sets.newHashSet("foo", "bar");
        final ProjectedEntityStatsResponse expectedResponse = ProjectedEntityStatsResponse.newBuilder()
            .setPaginationResponse(PaginationResponse.newBuilder()
                .setNextCursor("go go go"))
            .build();
        final PaginationParameters paginationParams = PaginationParameters.newBuilder()
                .setCursor("startCursor")
                .build();
        final EntityStatsPaginationParams entityStatsPaginationParams =
                mock(EntityStatsPaginationParams.class);

        final Map<Long, Set<Long>> entities = StatsTestUtils.createEntityGroupsMap(targetEntities);

        when(paginationParamsFactory.newPaginationParams(paginationParams)).thenReturn(entityStatsPaginationParams);
        when(mockProjectedStatsStore.getEntityStats(entities, targetCommodities, Collections.emptySet(), entityStatsPaginationParams))
                .thenReturn(expectedResponse);

        List<EntityGroup> entityGroups = entities.entrySet().stream()
            .map(entry -> EntityGroup.newBuilder()
                .setSeedEntity(entry.getKey())
                .addAllEntities(entry.getValue())
                .build())
            .collect(Collectors.toList());

        final ProjectedEntityStatsResponse response = clientStub.getProjectedEntityStats(
            ProjectedEntityStatsRequest.newBuilder()
                .setScope(EntityStatsScope.newBuilder()
                    .setEntityGroupList(EntityGroupList.newBuilder()
                        .addAllGroups(entityGroups)))
                .addAllCommodityName(targetCommodities)
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor("startCursor"))
                .build());

        assertThat(response, is(expectedResponse));

        verify(mockProjectedStatsStore).getEntityStats(entities, targetCommodities, Collections.emptySet(),
            entityStatsPaginationParams);
    }


    private DeletePlanStatsRequest createDeletePlanStatsRequest(long topologyContextId) {
        return DeletePlanStatsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();
    }

    private DeletePlanStatsRequest createDeletePlanStatsRequest() {
        return DeletePlanStatsRequest.newBuilder()
            .build();
    }

    /**
     * Test the invocation of saveClusterHeadroom api of the statsHistoryService.
     */
    @Test
    public void saveClusterHeadroom() throws Exception {
        long clusterId = 1L;
        long headroom = 20L;
        long numVMs = 25L;
        SaveClusterHeadroomRequest request = SaveClusterHeadroomRequest.newBuilder()
            .addClusterHeadroomInfo(ClusterHeadroomInfo.newBuilder()
                .setClusterId(clusterId)
                .setHeadroom(headroom))
            .build();
        clientStub.saveClusterHeadroom(request);
        verify(mockLoader, atLeastOnce()).insert(any());
        verify(mockLoader).flush(true);
    }

    /**
     * Test the invocation of the getClusterStats api of the StatsHistoryService.
     * Date range is not provided in the request. Get the latest records of the record
     * types requested.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testClusterStatsWithoutDates() throws Exception {
        String[] dates = {"2017-12-15 00:00:00"};
        String[] commodityNames = {HEADROOM_VMS, NUM_VMS};
        String clusterId = "1234567890";

        ClusterStatsRequestForHeadroomPlan request = ClusterStatsRequestForHeadroomPlan.newBuilder()
                .setClusterId(Long.parseLong(clusterId))
                .setStats(StatsFilter.newBuilder()
                        .addAllCommodityRequests(buildCommodityRequests(commodityNames))
                        .build())
                .build();

        when(mockClusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(anyLong(), anyLong(), anyLong(), any(), any()))
                .thenReturn(getMockStatRecords(clusterId, dates, commodityNames, 20));

        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getClusterStatsForHeadroomPlan(request).forEachRemaining(snapshots::add);

        assertThat(snapshots.size(), is(1));
        final StatSnapshot capturedArgument = snapshots.get(0);

        assertEquals(2, capturedArgument.getStatRecordsCount());
        List<StatRecord> startRecordList = capturedArgument.getStatRecordsList();
        List<String> recordNames = Arrays.asList(startRecordList.get(0).getName(),
                startRecordList.get(1).getName());
        assertThat(recordNames, containsInAnyOrder(HEADROOM_VMS, NUM_VMS));
    }

    /**
     * Test that the {@link StatsHistoryRpcService#getClusterStats}
     * behaves as expected: delegates the call to {@link ClusterStatsReader}.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testClusterStats() throws Exception {
        final ClusterStatsRequest request = ClusterStatsRequest.getDefaultInstance();
        final ClusterStatsResponse responseChunk = ClusterStatsResponse.getDefaultInstance();
        final List<ClusterStatsResponse> expectedResponse =
            new ArrayList<>();
        expectedResponse.add(responseChunk);
        when(mockClusterStatsReader.getStatsRecords(request)).thenReturn(expectedResponse);

        assertEquals(responseChunk,
            clientStub.getClusterStats(request).next());
        verify(mockClusterStatsReader).getStatsRecords(eq(request));
    }

    /**
     * Test the invocation of the getClusterStats api of the StatsHistoryService.
     * A date range is provided in the request. Verify all records within the range
     * are returned.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testClusterStatsWithDates() throws Exception {
        String[] dates = {"2017-12-13 00:00:00", "2017-12-14 00:00:00", "2017-12-15 00:00:00"};
        String[] commodityNames = {HEADROOM_VMS, NUM_VMS};

        long startDate = Timestamp.valueOf(dates[0]).getTime();
        long endDate = Timestamp.valueOf(dates[dates.length - 1]).getTime();
        String clusterId = "1234567890";

        ClusterStatsRequestForHeadroomPlan request = ClusterStatsRequestForHeadroomPlan.newBuilder()
                .setClusterId(Long.parseLong(clusterId))
                .setStats(StatsFilter.newBuilder()
                        .setStartDate(startDate)
                        .setEndDate(endDate)
                        .addAllCommodityRequests(buildCommodityRequests(commodityNames))
                        .build())
                .build();

        when(mockClusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(anyLong(), anyLong(), anyLong(), any(), any()))
                .thenReturn(getMockStatRecords(clusterId, dates, commodityNames, 20));

        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getClusterStatsForHeadroomPlan(request).forEachRemaining(snapshots::add);

        assertThat(snapshots.size(), is(3));
        for (int i = 0; i < 3; i++) {
            assertEquals(2, snapshots.get(i).getStatRecordsCount());
            List<StatRecord> startRecordList = snapshots.get(i).getStatRecordsList();
            List<String> recordNames = Arrays.asList(startRecordList.get(0).getName(),
                    startRecordList.get(1).getName());
            assertThat(recordNames, containsInAnyOrder(HEADROOM_VMS, NUM_VMS));
        }
    }

    /**
     * Verify that if the date range spans over a month, fetch data from the CLUSTER_STATS_BY_MONTH
     * table.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testClusterStatsByMonth() throws Exception {
        String[] commodityNames = {HEADROOM_VMS, NUM_VMS};
        long startDate = Timestamp.valueOf("2017-06-25 00:00:00").getTime();
        long endDate = Timestamp.valueOf("2017-12-19 00:00:00").getTime();
        String clusterId = "1234567890";

        ClusterStatsRequestForHeadroomPlan request = ClusterStatsRequestForHeadroomPlan.newBuilder()
                .setClusterId(Long.parseLong(clusterId))
                .setStats(StatsFilter.newBuilder()
                        .setStartDate(startDate)
                        .setEndDate(endDate)
                        .addAllCommodityRequests(buildCommodityRequests(commodityNames))
                        .build())
                .build();

        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getClusterStatsForHeadroomPlan(request).forEachRemaining(snapshots::add);

        assertThat(snapshots.size(), is(0));

        verify(mockClusterStatsReader, atLeastOnce())
            .getStatsRecordsForHeadRoomPlanRequest(eq(Long.parseLong(clusterId)),
                                                   eq(startDate), eq(endDate),
                                                   anyObject(), any());
    }

    /**
     * Test getting entity stats for any entity type with an entity type specified in scope.
     */
    @Test
    public void testGetEntityStats() throws VmtDbException {
        final EntityDTO.EntityType entityType = EntityType
                        .fromSdkEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE)
                        .get()
                        .getSdkEntityType()
                        .get();
        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                .setEntityList(EntityList.newBuilder()
                        .addEntities(1L))
                .build();
        final StatsFilter filter = StatsFilter.newBuilder()
                .setStartDate(100L)
                .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setCursor("foo")
                .build();
        final String retCursor = "bar";
        final Integer totalRecordCount = 100;

        // Mocks returning some entityType (Here VIRTUAL_MACHINE).
        final Map<Long, EntityDTO.EntityType> entityIdToTypeMap = ImmutableMap.of(1L, EntityDTO.EntityType.VIRTUAL_MACHINE);
        when(historyDbio.getEntityIdToEntityTypeMap(ImmutableSet.of(1L))).thenReturn(entityIdToTypeMap);
        when(historyDbio.getEntityTypeFromEntityStatsScope(scope)).thenReturn(EntityType.fromSdkEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE).get());
        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParamsFactory.newPaginationParams(paginationParameters)).thenReturn(paginationParams);
        final StatRecordPage statRecordPage = mock(StatRecordPage.class);
        final Record record = mock(Record.class);
        final Map<Long, List<Record>> recordPage = ImmutableMap.of(1L, Collections.singletonList(record));
        when(statRecordPage.getNextPageRecords()).thenReturn(recordPage);
        when(statRecordPage.getNextCursor()).thenReturn(Optional.of(retCursor));
        when(statRecordPage.getTotalRecordCount()).thenReturn(Optional.of(totalRecordCount));

        when(mockLivestatsreader.getPaginatedStatsRecords(scope, filter, paginationParams))
                .thenReturn(statRecordPage);
        final StatSnapshot.Builder statSnapshotBuilder = StatSnapshot.newBuilder()
                .setSnapshotDate(1L);
        doReturn(Stream.of(statSnapshotBuilder)).when(statSnapshotCreatorSpy)
                .createStatSnapshots(Collections.singletonList(record), false, Collections.emptyList());

        final GetEntityStatsResponse response = clientStub.getEntityStats(GetEntityStatsRequest.newBuilder()
                .setScope(scope)
                .setFilter(filter)
                .setPaginationParams(paginationParameters)
                .build());
        assertThat(response.getEntityStatsList(), contains(EntityStats.newBuilder()
                .setOid(1L)
                .addStatSnapshots(statSnapshotBuilder)
                .build()));
        assertThat(response.getPaginationResponse().getNextCursor(), is(retCursor));
        assertThat(response.getPaginationResponse().getTotalRecordCount(), is(totalRecordCount));
    }

    /**
     * Test getting entity stats for PhysicalMachines with an entity type specified in scope.
     *
     * Since PM and DC stats are stored in the same table, we need to make sure only PM stats are
     * retrieved when requesting PM stats.
     */
    @Test
    public void testGetEntityStatsForPhysicalMachine() throws VmtDbException {
        final List<Long> entityOids = ImmutableList.of(1L, 2L);
        final EntityDTO.EntityType entityType = EntityType
                        .fromSdkEntityType(EntityDTO.EntityType.PHYSICAL_MACHINE)
                        .get()
                        .getSdkEntityType()
                        .get();

        final EntityStatsScope scope = EntityStatsScope.newBuilder()
                        .setEntityList(EntityList.newBuilder()
                                        .addAllEntities(entityOids))
                        .setEntityType(entityType.ordinal())
                        .build();
        final StatsFilter filter = StatsFilter.newBuilder()
                .setStartDate(100L)
                .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setCursor("foo")
                .build();
        final String retCursor = "bar";
        final Integer totalRecordCount = 100;

        final Map<Long, EntityDTO.EntityType> entityIdToTypeMap = ImmutableMap.of(1L, EntityDTO.EntityType.PHYSICAL_MACHINE,
                                                                                  2L, EntityDTO.EntityType.DATACENTER);
        when(historyDbio.getEntityIdToEntityTypeMap(ImmutableSet.of(1L, 2L))).thenReturn(entityIdToTypeMap);
        when(historyDbio.getEntityTypeFromEntityStatsScope(scope)).thenReturn(EntityType
                        .fromSdkEntityType(EntityDTO.EntityType.PHYSICAL_MACHINE).get());
        final EntityStatsPaginationParams paginationParams =
                                                           mock(EntityStatsPaginationParams.class);
        when(paginationParamsFactory.newPaginationParams(paginationParameters)).thenReturn(paginationParams);
        final StatRecordPage statRecordPage = mock(StatRecordPage.class);
        // PM and DC entities are stored in the same pm_stats* tables, hence we need
        // to make sure we're only returning entities of the requested type, which in this case is PhysicalMachine.
        final Record record1 = mock(Record.class);
        final Record record2 = mock(Record.class);
        final Map<Long, List<Record>> recordPage = ImmutableMap.of(1L, ImmutableList.of(record1),
                                                                   2L, ImmutableList.of(record2));
        when(statRecordPage.getNextPageRecords()).thenReturn(recordPage);
        when(statRecordPage.getNextCursor()).thenReturn(Optional.of(retCursor));
        when(statRecordPage.getTotalRecordCount()).thenReturn(Optional.of(totalRecordCount));

        when(mockLivestatsreader.getPaginatedStatsRecords(scope, filter, paginationParams))
                .thenReturn(statRecordPage);
        final StatSnapshot.Builder statSnapshotBuilder1 = StatSnapshot.newBuilder()
                .setSnapshotDate(1L);
        final StatSnapshot.Builder statSnapshotBuilder2 = StatSnapshot.newBuilder()
                        .setSnapshotDate(2L);
        doReturn(Stream.of(statSnapshotBuilder1)).when(statSnapshotCreatorSpy)
                .createStatSnapshots(ImmutableList.of(record1), false, Collections.emptyList());
        doReturn(Stream.of(statSnapshotBuilder2)).when(statSnapshotCreatorSpy)
        .createStatSnapshots(ImmutableList.of(record2), false, Collections.emptyList());

        final GetEntityStatsResponse response = clientStub.getEntityStats(GetEntityStatsRequest.newBuilder()
                .setScope(scope)
                .setFilter(filter)
                .setPaginationParams(paginationParameters)
                .build());
        assertThat(response.getEntityStatsList(), contains(EntityStats.newBuilder()
                .setOid(1L)
                .addStatSnapshots(statSnapshotBuilder1)
                .build()));
        assertFalse(response.getEntityStatsList().contains(EntityStats.newBuilder()
                .setOid(2L)
                .addStatSnapshots(statSnapshotBuilder1)
                .build()));
        assertThat(response.getPaginationResponse().getNextCursor(), is(retCursor));
        assertThat(response.getPaginationResponse().getTotalRecordCount(), is(totalRecordCount));
    }

    /**
     * Generates a lists of fake stats records that belong to the given cluster ID, commodity names,
     * and on the given dates.
     *
     * @param clusterId cluster ID
     * @param dates an array of dates for the generated records
     * @param commodityNames commodity names (e.g. headroomVMs)
     * @param value value of the record
     * @return a new ClusterStatsByDayRecord
     */
    private List<ClusterStatsRecordReader> getMockStatRecords(String clusterId,
                                                             String[] dates,
                                                             String[] commodityNames,
                                                             int value) {
        List<ClusterStatsByDayRecord> results = Lists.newArrayList();

        for (String date : dates) {
            for (String commodityName : commodityNames) {
                ClusterStatsByDayRecord record = new ClusterStatsByDayRecord();
                record.setRecordedOn(Timestamp.valueOf(date));
                record.setInternalName(clusterId);
                record.setPropertyType(commodityName);
                record.setPropertySubtype(commodityName);
                record.setValue(Double.valueOf(value));
                results.add(record);
            }
        }
        return results.stream().map(ClusterStatsRecordReader::new).collect(Collectors.toList());
    }

    @Test
    public void testGetStatsDataRetentionSettings() throws VmtDbException {

        String retentionSettingName = "numRetainedHours";
        int retentionPeriod = 10;
        Setting expectedSetting =
            Setting.newBuilder()
                .setSettingSpecName(retentionSettingName)
                .setNumericSettingValue(
                    SettingDTOUtil.createNumericSettingValue(retentionPeriod))
                .build();
        when(historyDbio.getStatsRetentionSettings())
                .thenReturn(Collections.singletonList(expectedSetting));
        final List<Setting> responseSettings = new ArrayList<>();
        clientStub.getStatsDataRetentionSettings(GetStatsDataRetentionSettingsRequest.getDefaultInstance())
                .forEachRemaining(responseSettings::add);

        // Assert
        assertThat(responseSettings.size(), is(1));
        assertThat(expectedSetting, is(responseSettings.get(0)));
    }

    @Test
    public void testSetStatsDataRetentionSetting() throws VmtDbException {

        // Setup
        String retentionSettingName = "numRetainedHours";
        int retentionPeriod = 10;
        Setting expectedSetting =
            Setting.newBuilder()
                .setSettingSpecName(retentionSettingName)
                .setNumericSettingValue(
                    SettingDTOUtil.createNumericSettingValue(retentionPeriod))
                .build();
        when(historyDbio.setStatsDataRetentionSetting(retentionSettingName,
            retentionPeriod)).thenReturn(Optional.of(expectedSetting));

        // Act
        final SetStatsDataRetentionSettingResponse response =
            clientStub.setStatsDataRetentionSetting(SetStatsDataRetentionSettingRequest.newBuilder()
                .setRetentionSettingName(retentionSettingName)
                .setRetentionSettingValue(retentionPeriod)
                .build());

        // Assert
        assertTrue(response.hasNewSetting());
        assertThat(response.getNewSetting(), equalTo(expectedSetting));
    }

    @Test
    public void testSetStatsDataRetentionSettingMissingRequestParameters() {

        try {
            clientStub.setStatsDataRetentionSetting(
                    SetStatsDataRetentionSettingRequest.newBuilder()
                            .setRetentionSettingName("numRetainedHours")
                            .build());
        } catch (StatusRuntimeException e) {
            assertThat(e, GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT).anyDescription());
        }
    }

    @Test
    public void testSetStatsDataRetentionSettingFailure() throws Exception {
        String retentionSettingName = "numRetainedHours";
        int retentionPeriod = 10;
        VmtDbException dbException = new VmtDbException(
            VmtDbException.UPDATE_ERR, "Error updating db");

        doThrow(dbException).when(historyDbio).setStatsDataRetentionSetting(
                retentionSettingName, retentionPeriod);

        try {
            clientStub.setStatsDataRetentionSetting(
                    SetStatsDataRetentionSettingRequest.newBuilder()
                            .setRetentionSettingName(retentionSettingName)
                            .setRetentionSettingValue(retentionPeriod)
                            .build());
        } catch (StatusRuntimeException e) {
            assertThat(e, GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL).anyDescription());
        }
    }

    @Test
    public void testGetAuditLogDataRetentionSetting() throws VmtDbException {

        String retentionSettingName = "retained_days";
        int retentionPeriod = 10;
        Setting expectedSetting =
            Setting.newBuilder()
                .setSettingSpecName(retentionSettingName)
                .setNumericSettingValue(
                    SettingDTOUtil.createNumericSettingValue(retentionPeriod))
                .build();
        when(historyDbio.getAuditLogRetentionSetting())
                .thenReturn(expectedSetting);
        final GetAuditLogDataRetentionSettingResponse response =
                clientStub.getAuditLogDataRetentionSetting(GetAuditLogDataRetentionSettingRequest.getDefaultInstance());

        // Assert
        assertTrue(response.hasAuditLogRetentionSetting());
        assertThat(response.getAuditLogRetentionSetting(), equalTo(expectedSetting));
    }

    @Test
    public void testSetAuditLogDataRetentionSettingMissingRequestParameters() {
        try {
            clientStub.setAuditLogDataRetentionSetting(
                    SetAuditLogDataRetentionSettingRequest.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            assertThat(e, GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT).anyDescription());
        }
    }

    @Test
    public void testAuditLogDataRetentionSettingFailure() throws Exception {
        int retentionPeriod = 10;
        VmtDbException dbException = new VmtDbException(
            VmtDbException.UPDATE_ERR, "Error updating db");

        doThrow(dbException).when(historyDbio)
            .setAuditLogRetentionSetting(retentionPeriod);

        try {
            clientStub.setAuditLogDataRetentionSetting(
                    SetAuditLogDataRetentionSettingRequest.newBuilder()
                            .setRetentionSettingValue(retentionPeriod)
                            .build());
        } catch (StatusRuntimeException e) {
            assertThat(e, GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL).anyDescription());
        }
    }

    /**
     * Test getSystemLoadInfo without clusterIds.
     */
    @Test(expected = StatusRuntimeException.class)
    public void testGetNoSourceSystemLoadInfoError() {
        clientStub.getSystemLoadInfo(SystemLoadInfoRequest.newBuilder().build())
            .forEachRemaining(response -> { });
    }

    /**
     * Test getSystemLoadInfo with multiple clusterIds.
     */
    @Test
    public void testGetMultipleSourcesSystemLoadInfoSuccess() {
        final long clusterId1 = 1;
        final long clusterId2 = 2;
        List<SystemLoadRecord> records1 = Lists.newArrayList(
            newSystemLoadInfo(String.valueOf(clusterId1)));
        List<SystemLoadRecord> records2 = Lists.newArrayList(
            newSystemLoadInfo(String.valueOf(clusterId2)));
        doReturn(records1).when(systemLoadReader).getSystemLoadInfo(eq(String.valueOf(clusterId1)));
        doReturn(records2).when(systemLoadReader).getSystemLoadInfo(eq(String.valueOf(clusterId2)));

        clientStub.getSystemLoadInfo(
            SystemLoadInfoRequest.newBuilder().addClusterId(clusterId1).addClusterId(clusterId2).build())
            .forEachRemaining(response -> {
                final long clusterId = response.getClusterId();
                assertThat(newStatsSystemLoadInfo(clusterId), is(response.getRecordList().get(0)));
            });
    }

    /**
     * Test getSystemLoadInfo with multiple clusterIds with error.
     */
    @Test
    public void testGetMultipleSourcesSystemLoadInfoError() {
        final long clusterId1 = 1;
        final long clusterId2 = 2;
        List<SystemLoadRecord> records2 = Lists.newArrayList(
            newSystemLoadInfo(String.valueOf(clusterId2)));

        final RuntimeException error = new NullPointerException("one two three");
        doThrow(error)
            .when(systemLoadReader).getSystemLoadInfo(eq(String.valueOf(clusterId1)));
        doReturn(records2)
            .when(systemLoadReader).getSystemLoadInfo(eq(String.valueOf(clusterId2)));

        clientStub.getSystemLoadInfo(
            SystemLoadInfoRequest.newBuilder().addClusterId(clusterId1).addClusterId(clusterId2).build())
            .forEachRemaining(response -> {
                final long clusterId = response.getClusterId();
                if (clusterId == clusterId1) {
                    assertThat(response.getError(), is(error.getMessage()));
                } else {
                    assertThat(newStatsSystemLoadInfo(clusterId), is(response.getRecordList().get(0)));
                }
            });
    }

    /**
     * Tests returnStatsForEntityGroups setting {@link PaginationResponse} totalRecordCount.
     *
     * @throws VmtDbException if database errors occurs
     */
    @Test
    public void testReturnStatsForEntityGroupsSettingTotalRecordCountInPaginationResponse()
            throws VmtDbException {
        //WHEN
        EntityGroup eGroup1 = EntityGroup.newBuilder().setSeedEntity(1).build();
        EntityGroup eGroup2 = EntityGroup.newBuilder().setSeedEntity(2).build();
        EntityGroupList entityGroupList  = EntityGroupList.newBuilder().addGroups(eGroup1).addGroups(eGroup2).build();
        StatsFilter statsFilter = StatsFilter.newBuilder().build();
        PaginationParameters paginationParameters = PaginationParameters.newBuilder().build();

        //GIVEN
        statsHistoryRpcService.returnStatsForEntityGroups(entityGroupList, statsFilter, Optional.of(paginationParameters), this.getEntityStatsResponseStreamObserver);

        //THEN
        assertNotNull(this.getEntityStatsResponseStreamObserver.getGetEntityStatsResponse());
        assertNotNull(this.getEntityStatsResponseStreamObserver.getGetEntityStatsResponse().getPaginationResponse());
        assertTrue(this.getEntityStatsResponseStreamObserver.getGetEntityStatsResponse().getPaginationResponse().getTotalRecordCount() == 2);
    }

    private static final long VOLUME_ID_1 = 11111L;
    private static final long VOLUME_ID_2 = 22222L;
    private static final long INSTANT = 1605846218244L;
    private static final long INSTANT_2 = 1605846118244L;
    private static final long VM_ID_1 = 555555L;
    private static final long VM_ID_2 = 666666L;
    private static final String VM_1_NAME = "vm-1";

    /**
     * Test that GetVolumeAttachmentHistory returns correct response for single volume request
     * without VM name retrieval.
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeNoVmName() {
        final Record3<Long, Long, Date> record = createMockRecord(VOLUME_ID_1, VM_ID_1, INSTANT);
        when(volumeAttachmentHistoryReader.getVolumeAttachmentHistory(
            Collections.singletonList(VOLUME_ID_1)))
            .thenReturn(Collections.singletonList(record));

        final GetVolumeAttachmentHistoryRequest request =
            createRequest(Collections.singletonList(VOLUME_ID_1), false);
        final Iterator<GetVolumeAttachmentHistoryResponse> responseIterator =
            clientStub.getVolumeAttachmentHistory(request);

        verifyVolumeAttachmentHistoryResponse(VOLUME_ID_1, null, INSTANT, responseIterator.next());
    }

    /**
     * Test that GetVolumeAttachmentHistory returns correct response for single volume request
     * with VM name retrieval.
     */
    @Test
    public void testGetVolumeAttachmentHistorySingleVolumeWithVmName() {
        final Record3<Long, Long, Date> record = createMockRecord(VOLUME_ID_1, VM_ID_1, INSTANT_2);
        when(volumeAttachmentHistoryReader.getVolumeAttachmentHistory(
            Collections.singletonList(VOLUME_ID_1)))
            .thenReturn(Collections.singletonList(record));

        final GetVolumeAttachmentHistoryRequest request =
            createRequest(Collections.singletonList(VOLUME_ID_1), true);
        when(mockLivestatsreader.getEntityDisplayNameForId(VM_ID_1)).thenReturn(VM_1_NAME);
        final Iterator<GetVolumeAttachmentHistoryResponse> responseIterator =
            clientStub.getVolumeAttachmentHistory(request);

        verifyVolumeAttachmentHistoryResponse(VOLUME_ID_1, VM_1_NAME, INSTANT_2,
            responseIterator.next());
    }

    /**
     * Test that GetVolumeAttachmentHistory returns correct response for multiple volume request
     * (multiple volume requests are always expected to be without VM name retrieval).
     */
    @Test
    public void testGetVolumeAttachmentHistoryBulkVolumesNoVmName() {
        final Record3<Long, Long, Date> record1 = createMockRecord(VOLUME_ID_1, VM_ID_1, INSTANT);
        final Record3<Long, Long, Date> record2 = createMockRecord(VOLUME_ID_2, VM_ID_2, INSTANT_2);
        final List<Long> requestedVolumes = Stream.of(VOLUME_ID_1, VOLUME_ID_2)
            .collect(Collectors.toList());
        when(volumeAttachmentHistoryReader.getVolumeAttachmentHistory(requestedVolumes))
            .thenReturn(Stream.of(record1, record2).collect(Collectors.toList()));

        final GetVolumeAttachmentHistoryRequest request = createRequest(requestedVolumes, false);
        final Iterator<GetVolumeAttachmentHistoryResponse> responseIterator =
            clientStub.getVolumeAttachmentHistory(request);
        final GetVolumeAttachmentHistoryResponse response = responseIterator.next();

        verifyVolumeAttachmentHistoryResponse(VOLUME_ID_1, null, INSTANT, response);
        verifyVolumeAttachmentHistoryResponse(VOLUME_ID_2, null, INSTANT_2, response);
    }

    private void verifyVolumeAttachmentHistoryResponse(
        final long expectedVolumeOid, final String expectedVmName, final long expectedInstant,
        final GetVolumeAttachmentHistoryResponse actualResponse) {
        final Optional<VolumeAttachmentHistory> history = actualResponse.getHistoryList().stream()
            .filter(h -> h.getVolumeOid() == expectedVolumeOid)
            .findAny();
        Assert.assertTrue(history.isPresent());
        if (expectedVmName == null) {
            Assert.assertTrue(history.get().getVmNameList().isEmpty());
        } else {
            final String actualVmName = history.get().getVmNameList().iterator().next();
            Assert.assertEquals(expectedVmName, actualVmName);
        }
        Assert.assertEquals(expectedInstant, history.get().getLastAttachedDateMs());
    }

    private GetVolumeAttachmentHistoryRequest createRequest(final List<Long> volumeOids,
                                                            final boolean retrieveVmNames) {
        return Stats.GetVolumeAttachmentHistoryRequest
            .newBuilder()
            .addAllVolumeOid(volumeOids)
            .setRetrieveVmNames(retrieveVmNames)
            .build();
    }

    private Record3<Long, Long, Date> createMockRecord(final long volumeOid, final long vmOid,
                                                       final long instant) {
        final Record3<Long, Long, Date> record = mock(Record3.class);
        when(record.component1()).thenReturn(volumeOid);
        when(record.component2()).thenReturn(vmOid);
        when(record.component3()).thenReturn(new Date(instant));
        return record;
    }

    private static SystemLoadRecord newSystemLoadInfo(@Nonnull final String clusterId) {
        return new SystemLoadRecord(
            clusterId, SNAPSHOT_TIME, "2", null, "4", null, 1d, 2d, null, null, COMMODITIES, "6");
    }

    private static Stats.SystemLoadRecord newStatsSystemLoadInfo(final long clusterId) {
        return Stats.SystemLoadRecord.newBuilder().setClusterId(clusterId)
            .setSnapshotTime(SNAPSHOT_TIME.getTime()).setUuid(2).setProducerUuid(0)
            .setPropertyType("4").setPropertySubtype("").setCapacity(1).setAvgValue(2)
            .setMinValue(-1).setMaxValue(-1).setRelationType(COMMODITIES.ordinal())
            .setCommodityKey("6").build();
    }

    /**
     * Allows testing of {@link GetEntityStatsResponse}.
     */
    private static class GetEntityStatsResponseStreamObserver implements StreamObserver<GetEntityStatsResponse> {

        //GetEntityStatsResponse set from via observer onNext
        private GetEntityStatsResponse getEntityStatsResponse;

        public GetEntityStatsResponse getGetEntityStatsResponse() {
            return this.getEntityStatsResponse;
        }

        @Override
        public void onNext(GetEntityStatsResponse getEntityStatsResponse) {
            this.getEntityStatsResponse = getEntityStatsResponse;
        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onCompleted() {

        }
    }

    /**
     * Test {@link StatsHistoryRpcService#createProjectedHeadroomStats}.
     *
     * @throws Exception if there's any
     */
    @Test
    public void testCreateProjectedHeadroomStats() throws Exception {
        final long latestRecordDate = System.currentTimeMillis();
        final long startDate = latestRecordDate - TimeUnit.DAYS.toMillis(1);
        final long endDate = latestRecordDate + TimeUnit.DAYS.toMillis(1) * 2 + 1;
        final long clusterId = 1L;
        when(mockClusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(anyLong(), anyLong(), anyLong(), any(), any())).thenReturn(
            getMockStatRecords(String.valueOf(clusterId), new String[] {new Timestamp(latestRecordDate).toString()},
                new String[] {StringConstants.VM_GROWTH}, 30));

        float value = 10f;
        float capacity = 30f;
        final StatRecord statRecord = StatRecord.newBuilder()
            .setName(StringConstants.CPU_HEADROOM)
            .setUsed(StatValue.newBuilder().setAvg(value))
            .setCapacity(StatValue.newBuilder().setAvg(capacity)).build();

        final StreamObserver<StatSnapshot> responseStreamObserver = Mockito.mock(StreamObserver.class);
        final ArgumentCaptor<StatSnapshot> statSnapshotCaptor = ArgumentCaptor.forClass(StatSnapshot.class);

        statsHistoryRpcService.createProjectedHeadroomStats(responseStreamObserver,
            ClusterStatsRequestForHeadroomPlan.newBuilder().setClusterId(clusterId).setStats(
                StatsFilter.newBuilder().setStartDate(startDate).setEndDate(endDate)).build(),
            latestRecordDate, Collections.singletonList(statRecord));

        Mockito.verify(responseStreamObserver, times(4)).onNext(statSnapshotCaptor.capture());

        final List<StatSnapshot> projectedSnapshots = statSnapshotCaptor.getAllValues();
        assertThat(projectedSnapshots.size(), is(4));

        final StatSnapshot currentSnapshot = projectedSnapshots.get(0);
        assertThat(currentSnapshot.getStatEpoch(), is(StatEpoch.CURRENT));
        assertThat(currentSnapshot.getStatRecordsCount(), is(1));
        assertThat(currentSnapshot.getStatRecords(0), is(statRecord));

        for (int day = 1; day < projectedSnapshots.size(); day++) {
            final StatSnapshot projectedSnapshot = projectedSnapshots.get(day);
            assertThat(projectedSnapshot.getSnapshotDate(),
                is(Math.min(latestRecordDate + day * TimeUnit.DAYS.toMillis(1), endDate)));
            assertThat(projectedSnapshot.getStatEpoch(), is(StatEpoch.PROJECTED));
            assertThat(projectedSnapshot.getStatRecordsCount(), is(1));
            assertThat(projectedSnapshot.getStatRecords(0).getUsed().getAvg(), is(value - day));
        }
    }
}
