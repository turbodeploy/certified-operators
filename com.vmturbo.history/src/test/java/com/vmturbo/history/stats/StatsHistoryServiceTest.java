package com.vmturbo.history.stats;

import static com.vmturbo.reports.db.StringConstants.USED;
import static com.vmturbo.reports.db.StringConstants.UTILIZATION;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.assertj.core.util.Sets;
import org.jooq.Record;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.db.abstraction.tables.records.PmStatsLatestRecord;
import com.vmturbo.reports.db.abstraction.tables.records.ScenariosRecord;

/**
 * Test gRPC methods to handle snapshot requests.
 */
public class StatsHistoryServiceTest {

    private static final long PLAN_UUID = 1L;
    private static final long ENTITY_UUID = 2L;
    private static final long PLAN_OID = PLAN_UUID;
    private static final Timestamp SNAPSHOT_TIME = new Timestamp(123L);
    private static final long REALTIME_CONTEXT_ID = 7L;
    private StatsHistoryService statsHistoryService;
    private final long topologyContextId = 8L;

    @Mock
    private LiveStatsReader mockLivestatsreader;

    @Mock
    private PlanStatsReader mockPlanStatsReader;

    @Mock
    private ClusterStatsWriter mockClusterStatsWriter;

    @Mock
    private HistorydbIO historyDbio;

    @Mock
    private ProjectedStatsStore mockProjectedStatsStore;

    @Mock
    private StreamObserver<StatSnapshot> mockStatSnapshotStreamObserver;

    @Mock
    private StreamObserver<ProjectedStatsResponse> mockProjectedStatsStreamObserver;

    @Mock
    private StreamObserver<EntityStats> mockEntityStatsStreamObserver;

    @Mock
    private StreamObserver<SaveClusterHeadroomResponse> mockSaveClusterHeadroomStreamObserver;

    @Mock
    private StreamObserver<DeletePlanStatsResponse> mockDeletePlanStatsStreamObserver;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        statsHistoryService =  new StatsHistoryService(REALTIME_CONTEXT_ID,
                mockLivestatsreader, mockPlanStatsReader,
                mockClusterStatsWriter, historyDbio, mockProjectedStatsStore);
    }

    /**
     * Test the generated count statistics.
     */
    @Test
    public void testGetStatsCounts() throws Exception {
        // Arrange
        List<Long> entities = Arrays.asList(1L, 2L, 3L);
        List<Record> statsRecordsList = new ArrayList<>();

        // convert to the standard time format we return
        final String snapshotTimeTest = DateTimeUtil.toString(SNAPSHOT_TIME.getTime());
        // the two values for "c1" will be averaged"
        final float c1Value1 = 123;
        final String propType = "c1";
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, c1Value1, propType, USED);
        final float c1Value2 = 456;
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, c1Value2, propType, USED);
        final float c1Avg = (c1Value1 + c1Value2) / 2;
        // only one value for "c2"
        final float c2Value = 789;
        final String propType2 = "c2";
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, c2Value, propType2, USED);
        // This one (utilization) should be dropped while processing the stats.
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 0.95, propType2, UTILIZATION);

        when(mockLivestatsreader.getStatsRecords(anyObject(), anyObject(), anyObject(), anyObject()))
                .thenReturn(statsRecordsList);
        Stats.EntityStatsRequest.Builder testStatsRequest = Stats.EntityStatsRequest.newBuilder();
        testStatsRequest.addAllEntities(entities);

        // Act
        statsHistoryService.getAveragedEntityStats(testStatsRequest.build(),
                mockStatSnapshotStreamObserver);

        // Assert
        ArgumentCaptor<StatSnapshot> snapshotCaptor = ArgumentCaptor.forClass(StatSnapshot.class);
        verify(mockStatSnapshotStreamObserver).onNext(snapshotCaptor.capture());
        StatSnapshot snapshotObserved = snapshotCaptor.getValue();
        assertThat(snapshotObserved.getSnapshotDate(), is(snapshotTimeTest));
        assertThat(snapshotObserved.getStatRecordsCount(), is(2));

        // The order is not guaranteed as map iteration is used in the implementation.
        StatRecord statRecord = snapshotObserved.getStatRecords(0);
        StatRecord statRecord2 = snapshotObserved.getStatRecords(1);

        if (propType.equals(statRecord.getName())) {
            // statRecord is for c1 and statRecord2 is for c2
            checkStatRecord(propType, c1Avg, statRecord);
            checkStatRecord(propType2, c2Value, statRecord2);
        } else if (propType.equals(statRecord2.getName())) {
            // statRecord is for c2 and statRecord2 is for c1
            checkStatRecord(propType2, c2Value, statRecord);
            checkStatRecord(propType, c1Avg, statRecord2);
        } else {
            fail("Wrong stat records: " + snapshotObserved.getStatRecordsList());
        }
        verify(mockStatSnapshotStreamObserver).onCompleted();
        verifyNoMoreInteractions(mockStatSnapshotStreamObserver);
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
        StatsFilter.Builder reqStatsBuilder = StatsFilter.newBuilder()
                    .setStartDate(startDate)
                    .setEndDate(endDate);
        List<String> commodityNames = Lists.newArrayList("c1", "c2", "c3");
        reqStatsBuilder.addAllCommodityName(commodityNames);

        // full 'Market' stats request has no entities
        Stats.EntityStatsRequest testStatsRequest = Stats.EntityStatsRequest.newBuilder()
                .setFilter(reqStatsBuilder)
                .build();

        List<Record> statsRecordsList = new ArrayList<>();
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 1, "c1", "c1-subtype");
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 2, "c2", "c2-subtype");
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 3, "c3", "c3-subtype");
        when(mockLivestatsreader.getFullMarketStatsRecords(startDate, endDate, commodityNames))
                .thenReturn(statsRecordsList);

        // act
        statsHistoryService.getAveragedEntityStats(testStatsRequest, mockStatSnapshotStreamObserver);

        // assert
        verify(mockLivestatsreader).getFullMarketStatsRecords(eq(startDate), eq(endDate),
                anyObject());
        verify(mockStatSnapshotStreamObserver).onNext(anyObject());
        verify(mockStatSnapshotStreamObserver).onCompleted();
        verifyNoMoreInteractions(mockStatSnapshotStreamObserver);
        verifyNoMoreInteractions(mockPlanStatsReader);

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
        statsHistoryService.getAveragedEntityStats(EntityStatsRequest.newBuilder()
            .addEntities(PLAN_UUID)
            .setFilter(requestedStats)
            .build(), mockStatSnapshotStreamObserver);

        // assert
        verify(mockPlanStatsReader).getStatsRecords(eq(PLAN_OID), anyObject());
        verify(mockStatSnapshotStreamObserver).onNext(anyObject());
        verify(mockStatSnapshotStreamObserver).onCompleted();
        verifyNoMoreInteractions(mockStatSnapshotStreamObserver);
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
        final List<String> entityUuidsStr = Lists.newArrayList(Long.toString(ENTITY_UUID));

        StatsFilter.Builder reqStatsBuilder = StatsFilter.newBuilder()
            .setStartDate(startDate)
            .setEndDate(endDate);
        List<String> commodityNames = Lists.newArrayList("c1", "c2", "c3");
        reqStatsBuilder.addAllCommodityName(commodityNames);

        List<Record> statsRecordsList = new ArrayList<>();
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 1, "c1", "c1-subtype");
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 2, "c2", "c2-subtype");
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 3, "c3", "c3-subtype");
        when(mockLivestatsreader.getStatsRecords(eq(entityUuidsStr), eq(startDate), eq(endDate),
                eq(commodityNames))).thenReturn(statsRecordsList);

        Stats.EntityStatsRequest testStatsRequest = Stats.EntityStatsRequest.newBuilder()
                .addAllEntities(entityUuids)
                .setFilter(reqStatsBuilder)
                .build();
        // act
        statsHistoryService.getAveragedEntityStats(testStatsRequest, mockStatSnapshotStreamObserver);

        // assert
        verify(mockLivestatsreader).getStatsRecords(eq(entityUuidsStr), eq(startDate), eq(endDate),
                eq(commodityNames));
        verify(mockStatSnapshotStreamObserver).onNext(anyObject());
        verify(mockStatSnapshotStreamObserver).onCompleted();
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
        final List<String> queryEntityUuidsStr = queryEntityUuids.stream()
                .map(oid -> Long.toString(oid))
                .collect(Collectors.toList());

        StatsFilter.Builder reqStatsBuilder = StatsFilter.newBuilder()
                .setStartDate(startDate)
                .setEndDate(endDate);
        List<String> commodityNames = Lists.newArrayList("c1");
        reqStatsBuilder.addAllCommodityName(commodityNames);
        Stats.EntityStatsRequest testStatsRequest = Stats.EntityStatsRequest.newBuilder()
                .addAllEntities(queryEntityUuids)
                .setFilter(reqStatsBuilder)
                .build();

        // three rows for 'c1', values 1, 2, 3 respectively
        List<Record> statsRecordsList = new ArrayList<>();
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 1d, "c1", "c1-subtype");
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 2d, "c1", "c1-subtype");
        addStatsRecord(statsRecordsList, SNAPSHOT_TIME, 3d, "c1", "c1-subtype");

        when(mockLivestatsreader.getStatsRecords(eq(queryEntityUuidsStr), eq(startDate), eq(endDate),
                eq(commodityNames))).thenReturn(statsRecordsList);

        // act
        statsHistoryService.getAveragedEntityStats(testStatsRequest, mockStatSnapshotStreamObserver);

        // assert
        ArgumentCaptor<StatSnapshot> statSnapshotCaptor = ArgumentCaptor.forClass(StatSnapshot.class);
        verify(mockStatSnapshotStreamObserver).onNext(statSnapshotCaptor.capture());
        assertThat(statSnapshotCaptor.getAllValues().size(), equalTo(1));
        StatSnapshot statSnapshot = statSnapshotCaptor.getValue();
        List<StatRecord> snapshotRecords = statSnapshot.getStatRecordsList();
        verify(mockStatSnapshotStreamObserver).onCompleted();
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
        //      capacity = sum(3 x value) = 3, 6, 9; total 18
        assertThat(statRecord.getCapacity(), equalTo(18f));
    }

    /**
     * Create a Record to use in a response list. Use a PmStatsLatestRecord just as an example -
     * the type of the Record is not important. All of the different _stats_latest records have the
     * same schema.
     *
     * @param statsRecordsList the list to add the new record to
     * @param snapshotTime the time this stat was recorded
     * @param testValue the value of the stat
     * @param propType the property type for this stat
     * @param propSubType the property subtype for this stat
     */
    private void addStatsRecord(List<Record> statsRecordsList,
                                Timestamp snapshotTime,
                                double testValue,
                                String propType,
                                String propSubType) {
        PmStatsLatestRecord statsRecord = new PmStatsLatestRecord();
        statsRecord.setSnapshotTime(snapshotTime);
        statsRecord.setPropertyType(propType);
        statsRecord.setPropertySubtype(propSubType);
        statsRecord.setAvgValue(testValue);
        statsRecord.setMinValue(testValue / 2);
        statsRecord.setMaxValue(testValue * 2);
        statsRecord.setCapacity(testValue * 3);
        statsRecordsList.add(statsRecord);
    }

    @Test
    public void testDeletePlanStats() {
        statsHistoryService.deletePlanStats(
                createDeletePlanStatsRequest(topologyContextId),
                mockDeletePlanStatsStreamObserver);
        verify(mockDeletePlanStatsStreamObserver).onNext(anyObject());
        verify(mockDeletePlanStatsStreamObserver).onCompleted();
    }

    @Test
    public void testDeletePlanStatsMissingParameter() {

        statsHistoryService.deletePlanStats(
                createDeletePlanStatsRequest(),
                mockDeletePlanStatsStreamObserver);

        verify(mockDeletePlanStatsStreamObserver).onError(
            any(StatusRuntimeException.class));
    }

    @Test
    public void testDeletePlanStatsFailure() throws Exception {
        VmtDbException dbException = new VmtDbException(
            VmtDbException.DELETE_ERR, "Error deleting plan");

        doThrow(dbException).when(historyDbio)
            .deletePlanStats(topologyContextId);

        statsHistoryService.deletePlanStats(
            createDeletePlanStatsRequest(topologyContextId),
            mockDeletePlanStatsStreamObserver);

        verify(mockDeletePlanStatsStreamObserver).onError(
            any(VmtDbException.class));

        // TODO - karthikt : match exact arguments
        // verify(mockDeletePlanStatsStreamObserver).onError(
        //    Status.INTERNAL
        //    .withDescription("Error deleting plan stats with id: "
        //      + topologyContextId).asException());
    }


    @Test
    public void testGetProjectedStats() throws Exception {
        // arrange
        List<String> commodityNames = Lists.newArrayList();
        final ArrayList<Long> entityOids = Lists.newArrayList();
        ProjectedStatsRequest request = ProjectedStatsRequest.newBuilder()
                .addAllEntities(entityOids)
                .addAllCommodityName(commodityNames)
                .build();
        StatSnapshot statSnapshot = StatSnapshot.newBuilder()
                .build();
        when(mockProjectedStatsStore.getStatSnapshot(request))
                .thenReturn(Optional.of(statSnapshot));

        // act
        statsHistoryService.getProjectedStats(request, mockProjectedStatsStreamObserver);

        // assert
        ArgumentCaptor<ProjectedStatsResponse> projecteStatsResponseCaptor =
                ArgumentCaptor.forClass(ProjectedStatsResponse.class);
        verify(mockProjectedStatsStreamObserver).onNext(projecteStatsResponseCaptor.capture());
        final ProjectedStatsResponse response = projecteStatsResponseCaptor.getValue();
        assertTrue(response.hasSnapshot());
        StatSnapshot responseSnapshot = response.getSnapshot();
        assertThat(responseSnapshot, equalTo(statSnapshot));

        verify(mockProjectedStatsStreamObserver).onCompleted();
        verifyNoMoreInteractions(mockProjectedStatsStreamObserver);
    }

    /**
     * Request individual stats for 3 entities.
     * @throws Exception should never happen
     */
    @Test
    public void testGetProjectedEntityStats() throws Exception {
        // arrange
        List<String> commodityNames = Lists.newArrayList("c1", "c2");
        final ArrayList<Long> entityOids = Lists.newArrayList(1L, 2L, 3L);
        ProjectedStatsRequest request = ProjectedStatsRequest.newBuilder()
                .addAllEntities(entityOids)
                .addAllCommodityName(commodityNames)
                .build();
        StatSnapshot statSnapshot1 = StatSnapshot.newBuilder()
                .addStatRecords(StatRecord.newBuilder()
                        .setCurrentValue(1.0f)
                        .build())
                .build();
        StatSnapshot statSnapshot2 = StatSnapshot.newBuilder()
                .addStatRecords(StatRecord.newBuilder()
                        .setCurrentValue(2.0f)
                        .build())
                .build();
        StatSnapshot statSnapshot3 = StatSnapshot.newBuilder()
                .addStatRecords(StatRecord.newBuilder()
                        .setCurrentValue(3.0f)
                        .build())
                .build();

        final HashSet<String> commodityNamesSet = Sets.newHashSet(commodityNames);
        when(mockProjectedStatsStore.getStatSnapshotForEntities(Collections.singleton(1L),
                commodityNamesSet)).thenReturn(Optional.of(statSnapshot1));
        when(mockProjectedStatsStore.getStatSnapshotForEntities(Collections.singleton(2L),
                commodityNamesSet)).thenReturn(Optional.of(statSnapshot2));
        when(mockProjectedStatsStore.getStatSnapshotForEntities(Collections.singleton(3L),
                commodityNamesSet)).thenReturn(Optional.of(statSnapshot3));

        // act
        statsHistoryService.getProjectedEntityStats(request, mockEntityStatsStreamObserver);

        // assert
        ArgumentCaptor<EntityStats> entityStatsResponseCaptor =
                ArgumentCaptor.forClass(EntityStats.class);
        verify(mockEntityStatsStreamObserver, times(3)).onNext(entityStatsResponseCaptor.capture());
        List<EntityStats> responseValues = entityStatsResponseCaptor.getAllValues();
        assertThat(responseValues.size(), is(3));
        assertThat(responseValues.get(0).getOid(), is(1L));
        assertThat(responseValues.get(0).getStatSnapshotsCount(), is(1));
        assertThat(responseValues.get(0).getStatSnapshotsList().get(0), is(statSnapshot1));

        assertThat(responseValues.get(1).getOid(), is(2L));
        assertThat(responseValues.get(1).getStatSnapshotsCount(), is(1));
        assertThat(responseValues.get(1).getStatSnapshotsList().get(0), is(statSnapshot2));

        assertThat(responseValues.get(2).getOid(), is(3L));
        assertThat(responseValues.get(2).getStatSnapshotsCount(), is(1));
        assertThat(responseValues.get(2).getStatSnapshotsList().get(0), is(statSnapshot3));

        verify(mockEntityStatsStreamObserver).onCompleted();
        verifyNoMoreInteractions(mockEntityStatsStreamObserver);
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
     *
     * @throws Exception
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

        statsHistoryService.saveClusterHeadroom(request, mockSaveClusterHeadroomStreamObserver);

        verify(mockClusterStatsWriter).insertClusterStatsByDayRecord(clusterId,
                "headroomVMs", "headroomVMs", BigDecimal.valueOf(headroom));
        verify(mockClusterStatsWriter).insertClusterStatsByDayRecord(clusterId,
                "numVMs", "numVMs", BigDecimal.valueOf(numVMs));
        verify(mockSaveClusterHeadroomStreamObserver).onNext(anyObject());
        verify(mockSaveClusterHeadroomStreamObserver).onCompleted();
    }
}
