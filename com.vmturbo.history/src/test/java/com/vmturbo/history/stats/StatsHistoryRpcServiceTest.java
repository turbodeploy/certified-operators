package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.StringConstants.USED;
import static com.vmturbo.history.schema.StringConstants.UTILIZATION;
import static com.vmturbo.history.stats.StatsTestUtils.newStatRecord;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.jooq.Record;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetPaginationEntityByUtilizationRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetPaginationEntityByUtilizationResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetStatsDataRetentionSettingsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.StringConstants;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.history.schema.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.StatSnapshotCreator.DefaultStatSnapshotCreator;
import com.vmturbo.history.stats.live.LiveStatsReader;
import com.vmturbo.history.stats.live.LiveStatsReader.StatRecordPage;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test gRPC methods to handle snapshot requests.
 */
public class StatsHistoryRpcServiceTest {

    private static final long PLAN_UUID = 1L;
    private static final long ENTITY_UUID = 2L;
    private static final long PLAN_OID = PLAN_UUID;
    private static final Timestamp SNAPSHOT_TIME = new Timestamp(123L);
    private static final long REALTIME_CONTEXT_ID = 7L;
    private final long topologyContextId = 8L;

    private LiveStatsReader mockLivestatsreader = mock(LiveStatsReader.class);

    private PlanStatsReader mockPlanStatsReader = mock(PlanStatsReader.class);

    private ClusterStatsReader mockClusterStatsReader = mock(ClusterStatsReader.class);

    private ClusterStatsWriter mockClusterStatsWriter = mock(ClusterStatsWriter.class);

    private HistorydbIO historyDbio = mock(HistorydbIO.class);

    private ProjectedStatsStore mockProjectedStatsStore = mock(ProjectedStatsStore.class);

    private EntityStatsPaginationParamsFactory paginationParamsFactory =
            mock(EntityStatsPaginationParamsFactory.class);

    private StatRecordBuilder statRecordBuilderSpy = spy(new DefaultStatRecordBuilder(mockLivestatsreader));

    private StatSnapshotCreator statSnapshotCreatorSpy = spy(new DefaultStatSnapshotCreator(statRecordBuilderSpy));

    private StatsHistoryRpcService statsHistoryRpcService =
            new StatsHistoryRpcService(REALTIME_CONTEXT_ID,
                     mockLivestatsreader, mockPlanStatsReader,
                     mockClusterStatsReader, mockClusterStatsWriter,
                     historyDbio, mockProjectedStatsStore,
                    paginationParamsFactory,
                    statSnapshotCreatorSpy,
                    statRecordBuilderSpy);

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
        final String snapshotTimeTest = DateTimeUtil.toString(SNAPSHOT_TIME.getTime());
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
            newStatRecord(SNAPSHOT_TIME, c2Value, propType2, USED),
            // This one (utilization) should be dropped while processing the stats.
            newStatRecord(SNAPSHOT_TIME, 0.95, propType2, UTILIZATION));

        when(mockLivestatsreader.getStatsRecords(anyObject(), anyObject()))
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
        assertThat(snapshot.getSnapshotDate(), is(snapshotTimeTest));
        assertThat(snapshot.getStatRecordsCount(), is(2));

        // The order is not guaranteed as map iteration is used in the implementation.
        StatRecord statRecord = snapshot.getStatRecords(0);
        StatRecord statRecord2 = snapshot.getStatRecords(1);

        if (propType.equals(statRecord.getName())) {
            // statRecord is for c1 and statRecord2 is for c2
            checkStatRecord(propType, c1Avg, statRecord);
            checkStatRecord(propType2, c2Value, statRecord2);
        } else if (propType.equals(statRecord2.getName())) {
            // statRecord is for c2 and statRecord2 is for c1
            checkStatRecord(propType2, c2Value, statRecord);
            checkStatRecord(propType, c1Avg, statRecord2);
        } else {
            fail("Wrong stat records: " + snapshot.getStatRecordsList());
        }
    }

    /**
     * Check the values for a stat record - property type, avg, min, and max.
     *
     * @param propType the property type string
     * @param c1Value the value to test
     * @param statRecord the record to check
     */
    private void checkStatRecord(String propType, float c1Value,
                                 StatRecord statRecord) {
        assertThat(statRecord.getName(), is(propType));
        assertThat(statRecord.getValues().getAvg(), is(c1Value));
        assertThat(statRecord.getValues().getMin(), is(c1Value / 2));
        assertThat(statRecord.getValues().getMax(), is(c1Value * 2));
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
                Optional.empty()))
                .thenReturn(statsRecordsList);

        // act
        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getAveragedEntityStats(testStatsRequest).forEachRemaining(snapshots::add);

        // assert
        assertThat(snapshots.size(), is(1));
        verify(mockLivestatsreader).getFullMarketStatsRecords(eq(reqStatsBuilder.build()), anyObject());
        verifyNoMoreInteractions(mockPlanStatsReader);

    }

    private List<CommodityRequest> buildCommodityRequests(String ...commodityNames){
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
        verify(mockPlanStatsReader).getStatsRecords(eq(PLAN_OID), anyObject());
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
        when(mockLivestatsreader.getStatsRecords(eq(entityUuidsStr), eq(reqStatsBuilder.build())))
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
        verify(mockLivestatsreader).getStatsRecords(eq(entityUuidsStr), eq(reqStatsBuilder.build()));
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
        when(historyDbio.getMostRecentTimestamp()).thenReturn(Optional.of(new Timestamp(123L)));

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
            newStatRecord(SNAPSHOT_TIME, 1d, "c1", "c1-subtype"),
            newStatRecord(SNAPSHOT_TIME, 2d, "c1", "c1-subtype"),
            newStatRecord(SNAPSHOT_TIME, 3d, "c1", "c1-subtype"));

        when(mockLivestatsreader.getStatsRecords(eq(queryEntityUuidsStr), eq(reqStatsBuilder.build())))
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
        //      max = 2 x value = 2, 4, 6; avgMax = 4;
        assertThat(statRecord.getValues().getMax(), equalTo(4f));
        //      min = 0.5 x value = 0.5, 1.0, 1.5; avgMin = 1.0;
        assertThat(statRecord.getValues().getMin(), equalTo(1f));
        // in this case current := avgMax since subtype != type
        assertThat(statRecord.getCurrentValue(), equalTo(4f));
        assertThat(statRecord.getUsed().getTotal(), equalTo(6f));
        //      capacity = sum(3 x value) = 3, 6, 9; total 18
        assertThat(statRecord.getCapacity().getMin(), equalTo(3f));
        assertThat(statRecord.getCapacity().getMax(), equalTo(9f));
        assertThat(statRecord.getCapacity().getAvg(), equalTo(6f));
        assertThat(statRecord.getCapacity().getTotal(), equalTo(18f));
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
        when(mockProjectedStatsStore.getStatSnapshotForEntities(entityOids, commodityNames))
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

        when(paginationParamsFactory.newPaginationParams(paginationParams)).thenReturn(entityStatsPaginationParams);
        when(mockProjectedStatsStore.getEntityStats(targetEntities, targetCommodities, entityStatsPaginationParams))
                .thenReturn(expectedResponse);

        final ProjectedEntityStatsResponse response = clientStub.getProjectedEntityStats(
            ProjectedEntityStatsRequest.newBuilder()
                .addAllEntities(targetEntities)
                .addAllCommodityName(targetCommodities)
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor("startCursor"))
                .build());

        assertThat(response, is(expectedResponse));

        verify(mockProjectedStatsStore)
                .getEntityStats(targetEntities, targetCommodities, entityStatsPaginationParams);
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
                .setClusterId(clusterId)
                .setHeadroom(headroom)
                .setNumVMs(numVMs)
                .build();

        clientStub.saveClusterHeadroom(request);

        verify(mockClusterStatsWriter).insertClusterStatsByDayRecord(clusterId,
                "headroomVMs", "headroomVMs", BigDecimal.valueOf(headroom));
        verify(mockClusterStatsWriter).insertClusterStatsByDayRecord(clusterId,
                "numVMs", "numVMs", BigDecimal.valueOf(numVMs));
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
        String[] dates = {"2017-12-15"};
        String[] commodityNames = {"headroomVMs", "numVMs"};
        String clusterId = "1234567890";

        ClusterStatsRequest request = ClusterStatsRequest.newBuilder()
                .setClusterId(Long.parseLong(clusterId))
                .setStats(StatsFilter.newBuilder()
                        .addAllCommodityRequests(buildCommodityRequests(commodityNames))
                        .build())
                .build();

        when(mockClusterStatsReader.getStatsRecordsByDay(any(), any(), any(), any()))
                .thenReturn(getMockStatRecords(clusterId, dates, commodityNames));

        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getClusterStats(request).forEachRemaining(snapshots::add);

        assertThat(snapshots.size(), is(1));
        final StatSnapshot capturedArgument = snapshots.get(0);

        assertEquals(2, capturedArgument.getStatRecordsCount());
        List<StatRecord> startRecordList = capturedArgument.getStatRecordsList();
        List<String> recordNames = Arrays.asList(startRecordList.get(0).getName(),
                startRecordList.get(1).getName());
        assertThat(recordNames, containsInAnyOrder("headroomVMs", "numVMs"));
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
        String[] dates = {"2017-12-13", "2017-12-14", "2017-12-15"};
        String[] commodityNames = {"headroomVMs", "numVMs"};

        long startDate = Date.valueOf(dates[0]).getTime();
        long endDate = Date.valueOf(dates[dates.length - 1]).getTime();
        String clusterId = "1234567890";

        ClusterStatsRequest request = ClusterStatsRequest.newBuilder()
                .setClusterId(Long.parseLong(clusterId))
                .setStats(StatsFilter.newBuilder()
                        .setStartDate(startDate)
                        .setEndDate(endDate)
                        .addAllCommodityRequests(buildCommodityRequests(commodityNames))
                        .build())
                .build();

        when(mockClusterStatsReader.getStatsRecordsByDay(any(), any(), any(), any()))
                .thenReturn(getMockStatRecords(clusterId, dates, commodityNames));

        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getClusterStats(request).forEachRemaining(snapshots::add);

        assertThat(snapshots.size(), is(3));
        for (int i = 0; i < 3; i++) {
            assertEquals(2, snapshots.get(i).getStatRecordsCount());
            List<StatRecord> startRecordList = snapshots.get(i).getStatRecordsList();
            List<String> recordNames = Arrays.asList(startRecordList.get(0).getName(),
                    startRecordList.get(1).getName());
            assertThat(recordNames, containsInAnyOrder("headroomVMs", "numVMs"));
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
        String[] commodityNames = {"headroomVMs", "numVMs"};
        long startDate = Date.valueOf("2017-06-25").getTime();
        long endDate = Date.valueOf("2017-12-19").getTime();
        String clusterId = "1234567890";

        ClusterStatsRequest request = ClusterStatsRequest.newBuilder()
                .setClusterId(Long.parseLong(clusterId))
                .setStats(StatsFilter.newBuilder()
                        .setStartDate(startDate)
                        .setEndDate(endDate)
                        .addAllCommodityRequests(buildCommodityRequests(commodityNames))
                        .build())
                .build();

        final List<StatSnapshot> snapshots = new ArrayList<>();
        clientStub.getClusterStats(request).forEachRemaining(snapshots::add);

        assertThat(snapshots.size(), is(0));

        verify(mockClusterStatsReader).getStatsRecordsByMonth(eq(Long.parseLong(clusterId)),
                eq(startDate), eq(endDate), anyObject());
    }

    @Test
    public void testGetEntityStats() throws VmtDbException {
        final StatsFilter filter = StatsFilter.newBuilder()
                .setStartDate(100L)
                .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setCursor("foo")
                .build();
        final String retCursor = "bar";

        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParamsFactory.newPaginationParams(paginationParameters)).thenReturn(paginationParams);
        final StatRecordPage statRecordPage = mock(StatRecordPage.class);
        final Record record = mock(Record.class);
        final Map<Long, List<Record>> recordPage = ImmutableMap.of(1L, Collections.singletonList(record));
        when(statRecordPage.getNextPageRecords()).thenReturn(recordPage);
        when(statRecordPage.getNextCursor()).thenReturn(Optional.of(retCursor));

        when(mockLivestatsreader.getPaginatedStatsRecords(Collections.singleton("1"), filter, paginationParams))
                .thenReturn(statRecordPage);
        final StatSnapshot.Builder statSnapshotBuilder = StatSnapshot.newBuilder()
                .setSnapshotDate("date to uniquely identify this snapshot");
        doReturn(Stream.of(statSnapshotBuilder)).when(statSnapshotCreatorSpy)
                .createStatSnapshots(Collections.singletonList(record), false, Collections.emptyList());

        final GetEntityStatsResponse response = clientStub.getEntityStats(GetEntityStatsRequest.newBuilder()
                .addEntities(1L)
                .setFilter(filter)
                .setPaginationParams(paginationParameters)
                .build());
        assertThat(response.getEntityStatsList(), contains(EntityStats.newBuilder()
                .setOid(1L)
                .addStatSnapshots(statSnapshotBuilder)
                .build()));
        assertThat(response.getPaginationResponse().getNextCursor(), is(retCursor));
    }

    /**
     * Generates a lists of fake stats records that belong to the given cluster ID, commodity names,
     * and on the given dates.
     *
     * @param clusterId cluster ID
     * @param dates an array of dates for the generated records
     * @param commodityNames commodity names (e.g. headroomVMs)
     * @return a new ClusterStatsByDayRecord
     */
    private List<ClusterStatsByDayRecord> getMockStatRecords(String clusterId,
                                                             String[] dates,
                                                             String[] commodityNames) {
        List<ClusterStatsByDayRecord> results = Lists.newArrayList();

        for (String date : dates) {
            for (String commodityName : commodityNames) {
                ClusterStatsByDayRecord record = new ClusterStatsByDayRecord();
                record.setRecordedOn(Date.valueOf(date));
                record.setInternalName(clusterId);
                record.setPropertyType(commodityName);
                record.setPropertySubtype(commodityName);
                record.setValue(BigDecimal.valueOf(20));
                results.add(record);
            }
        }
        return results;
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

    @Test
    public void testGetPaginationEntityByUtilization() throws Exception {
        final GetPaginationEntityByUtilizationRequest request =
                GetPaginationEntityByUtilizationRequest.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setIsGlobal(false)
                        .addAllEntityIds(Lists.newArrayList(1L, 2L))
                        .setPaginationParams(PaginationParameters.newBuilder()
                                .setLimit(20))
                .build();
        when(historyDbio.paginateEntityByPriceIndex(request.getEntityIdsList(), request.getEntityType(),
                PaginationParameters.newBuilder(request.getPaginationParams())
                        .setLimit(request.getPaginationParams().getLimit() + 1)
                        .build(),
                request.getIsGlobal())).thenReturn(Lists.newArrayList(1L, 2L));
        final GetPaginationEntityByUtilizationResponse response =
                clientStub.getPaginationEntityByUtilization(request);
        assertEquals(2L, response.getEntityIdsCount());
        assertTrue(response.getEntityIdsList().containsAll(Lists.newArrayList(1L, 2L)));
        assertFalse(response.getPaginationResponse().hasNextCursor());
    }
}
