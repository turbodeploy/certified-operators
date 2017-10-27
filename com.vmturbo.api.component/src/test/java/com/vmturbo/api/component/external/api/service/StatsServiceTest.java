package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.reports.db.RelationType;

@RunWith(MockitoJUnitRunner.class)
public class StatsServiceTest {

    private StatsService statsService;

    private UuidMapper uuidMapper = Mockito.mock(UuidMapper.class);

    private TestStatsHistoryService testStatsHistoryService = spy(new TestStatsHistoryService());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);

    private Clock mockClock = Mockito.mock(Clock.class);


    private final String oid1 = "1";
    private final ApiId apiId1 = mock(ApiId.class);
    private final String oid2 = "2";
    private final ApiId apiId2 = mock(ApiId.class);

    final private static ImmutableList<String> recordList1 = ImmutableList.of(
                                    "CPU",
                                    "StorageLatency",
                                    "nextStepRoi",
                                    "ApplicationCommodity");

    final private static ImmutableList<String> recordList2 = ImmutableList.of(
                                    "nextStepRoi",
                                    "currentProfitMargin",
                                    "currentExpenses",
                                    "nextStepExpenses",
                                    "ActionPermit",
                                    "Space",
                                    "Extent",
                                    "ApplicationCommodity",
                                    "ClusterCommodity",
                                    "DataCenterCommodity",
                                    "DatastoreCommodity",
                                    "DSPMAccessCommodity",
                                    "NetworkCommodity",
                                    "SegmentationCommodity",
                                    "DrsSegmentationCommodity",
                                    "StorageClusterCommodity",
                                    "VAppAccessCommodity",
                                    "VDCCommodity",
                                    "VMPMAccessCommodity");

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(testStatsHistoryService,
            groupServiceSpy);

    @Before
    public void setUp() throws IOException {
        StatsHistoryServiceBlockingStub statsServiceRpc = StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
        groupExpander = Mockito.mock(GroupExpander.class);

        statsService = new StatsService(statsServiceRpc,
                repositoryApi, groupExpander, mockClock);

        when(uuidMapper.fromUuid(oid1)).thenReturn(apiId1);
        when(uuidMapper.fromUuid(oid2)).thenReturn(apiId2);
        when(apiId1.uuid()).thenReturn(oid1);
        when(apiId1.oid()).thenReturn(Long.parseLong(oid1));
        when(apiId2.uuid()).thenReturn(oid2);
        when(apiId2.oid()).thenReturn(Long.parseLong(oid2));
    }

    @Test
    public void testGetStatsByEntityQueryWithFiltering() throws Exception {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        final Set<Long> expandedOidList = Sets.newHashSet(apiId1.oid());
        when(groupExpander.expandUuid(anyObject())).thenReturn(expandedOidList);

        List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid1, inputDto);

        // The returned stats contain cpu, latency, roi, and app.
        // Should only keep cpu and latency and filter out roi and app.
        assertEquals(1, resp.size());
        List<StatApiDTO> stats = resp.get(0).getStatistics();
        assertEquals(2, stats.size());
        assertEquals(Sets.newHashSet("CPU", "StorageLatency"),
                            Sets.newHashSet(stats.get(0).getName(), stats.get(1).getName()));
    }

    @Test
    public void testGetStatsByEntityQueryWithAllFiltered() throws Exception {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        List<StatSnapshotApiDTO> resp = statsService.getStatsByEntityQuery(oid2, inputDto);

        // The returned stats will be all filtered out.
        assertEquals(0, resp.size());
    }

    @Test
    public void testGetClusterStats() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        final Set<Long> listOfOidsInGroup = Sets.newHashSet(apiId2.oid());
        when(groupExpander.expandUuid(anyObject())).thenReturn(listOfOidsInGroup);

        statsService.getStatsByEntityQuery(oid1, inputDto);

        ArgumentCaptor<EntityStatsRequest> requestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);

        verify(testStatsHistoryService).getAveragedEntityStats(requestCaptor.capture(), any());
        assertEquals(apiId1.oid(), requestCaptor.getValue().getEntitiesList().size());
        assertEquals(apiId2.oid(), (long)requestCaptor.getValue().getEntitiesList().get(0));
    }

    @Test
    public void testGetGroupStats() throws Exception {
        final StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();

        final Set<Long> listOfOidsInGroup = Sets.newHashSet(7L, 8L);
        when(groupExpander.expandUuid(anyObject())).thenReturn(listOfOidsInGroup);

        statsService.getStatsByEntityQuery(oid1, inputDto);

        ArgumentCaptor<EntityStatsRequest> requestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(testStatsHistoryService).getAveragedEntityStats(requestCaptor.capture(), any());
        assertThat(requestCaptor.getValue().getEntitiesList(), containsInAnyOrder(7L, 8L));
    }

    @Test
    public void testGetHistoricalStatsByEntityQuery() throws Exception {
        // arrange
        StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(2000L, "1000",
                "1500", "a");

        // just a simple SE, not group or cluster; expanded list is just the input OID
        when(groupExpander.expandUuid(oid1)).thenReturn(Sets.newHashSet(1L));

        // act
        List<StatSnapshotApiDTO> result = statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        ArgumentCaptor<EntityStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(testStatsHistoryService).getAveragedEntityStats(entityRequestCaptor.capture(),
                anyObject());
        assertThat(entityRequestCaptor.getAllValues().size(), equalTo(1));
        EntityStatsRequest entityStatsRequest = entityRequestCaptor.getAllValues().iterator().next();

        verify(testStatsHistoryService, times(0)).getProjectedStats(anyObject(), anyObject());
    }

    public StatPeriodApiInputDTO buildStatPeriodApiInputDTO(long currentDate, String startDate, String endDate, String statName) {
        StatPeriodApiInputDTO inputDto = new StatPeriodApiInputDTO();
        inputDto.setStartDate(startDate);
        inputDto.setEndDate(endDate);
        when(mockClock.millis()).thenReturn(currentDate);
        List<StatApiInputDTO> statisticsRequested = new ArrayList<>();
        StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
        statApiInputDTO.setName(statName);
        statisticsRequested.add(statApiInputDTO);
        inputDto.setStatistics(statisticsRequested);
        return inputDto;
    }

    @Test
    public void testGetProjectedStatsByEntityQuery() throws Exception {
        // arrange
        // request is in the future
        StatPeriodApiInputDTO inputDto = buildStatPeriodApiInputDTO(2000L, "2500",
                "2500", "a");

        // just a simple SE, not group or cluster; expanded list is just the input OID
        when(groupExpander.expandUuid(oid1)).thenReturn(Sets.newHashSet(1L));

        // act
        List<StatSnapshotApiDTO> result = statsService.getStatsByEntityQuery(oid1, inputDto);

        // assert
        verify(testStatsHistoryService, times(0)).getAveragedEntityStats(anyObject(),
                anyObject());

        ArgumentCaptor<ProjectedStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(ProjectedStatsRequest.class);

        verify(testStatsHistoryService, times(1)).getProjectedStats(entityRequestCaptor.capture(),
                anyObject());
    }

    @Test
    public void testGetStatsByUuidsQueryHistorical() throws Exception {

        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1", "2"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "1000",
                "1000", "a");
        inputDto.setPeriod(period);
        when(groupExpander.expandUuids(anySetOf(String.class))).thenReturn(Sets.newHashSet(1L, 2L));

        Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap = ImmutableMap.of(
                1L, Optional.of(new ServiceEntityApiDTO()),
                2L, Optional.of(new ServiceEntityApiDTO()));
        when(repositoryApi.getServiceEntitiesById(anySetOf(Long.class))).thenReturn(serviceEntityMap);

        // act
        List<EntityStatsApiDTO> result = statsService.getStatsByUuidsQuery(inputDto);

        // Assert
        assertThat(result.size(), equalTo(2));
        ArgumentCaptor<EntityStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(testStatsHistoryService, times(1)).getEntityStats(entityRequestCaptor.capture(),
                anyObject());
        EntityStatsRequest entityStatsRequest = entityRequestCaptor.getValue();
        assertThat(entityStatsRequest.getEntitiesList().size(), equalTo(2));
        assertThat(entityStatsRequest.getEntitiesList(), contains(1L, 2L));


        verify(testStatsHistoryService, times(0)).getProjectedStats(anyObject(),
                anyObject());

    }

    @Test
    public void testGetStatsByUuidsQueryProjected() throws Exception {

        StatScopesApiInputDTO inputDto = new StatScopesApiInputDTO();
        inputDto.setScopes(Lists.newArrayList("1", "2"));
        StatPeriodApiInputDTO period = buildStatPeriodApiInputDTO(2000L, "2500",
                "2500", "a");
        inputDto.setPeriod(period);
        when(groupExpander.expandUuids(anySetOf(String.class))).thenReturn(Sets.newHashSet(1L, 2L));

        Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap = ImmutableMap.of(
                1L, Optional.of(new ServiceEntityApiDTO()),
                2L, Optional.of(new ServiceEntityApiDTO()));
        when(repositoryApi.getServiceEntitiesById(anySetOf(Long.class))).thenReturn(serviceEntityMap);

        // act
        List<EntityStatsApiDTO> result = statsService.getStatsByUuidsQuery(inputDto);

        // Assert
        assertThat(result.size(), equalTo(2));
        ArgumentCaptor<ProjectedStatsRequest> entityRequestCaptor =
                ArgumentCaptor.forClass(ProjectedStatsRequest.class);
        verify(testStatsHistoryService, times(1)).getProjectedEntityStats(entityRequestCaptor.capture(),
                anyObject());
        ProjectedStatsRequest projectedStatsRequest = entityRequestCaptor.getValue();
        assertThat(projectedStatsRequest.getEntitiesList().size(), equalTo(2));
        assertThat(projectedStatsRequest.getEntitiesList(), contains(1L, 2L));

        verify(testStatsHistoryService, times(0)).getEntityStats(anyObject(),
                anyObject());

    }

    private class TestStatsHistoryService extends StatsHistoryServiceGrpc.StatsHistoryServiceImplBase {

        @Override
        public void getClusterStats(ClusterStatsRequest request,
                                    StreamObserver<StatSnapshot> responseObserver) {
            responseObserver.onCompleted();
        }

        @Override
        public void getAveragedEntityStats(EntityStatsRequest request,
                                   StreamObserver<StatSnapshot> responseObserver) {
            if (request.getEntitiesList() == null || request.getEntitiesList().isEmpty()) {
                responseObserver.onCompleted();
                return;
            }

            final long entityOid = request.getEntitiesList().get(0);

            if (Long.parseLong(oid1) == entityOid) {
                // nextStepRoi and ApplicationCommodity will be filtered out.
                final StatSnapshot stat = StatSnapshot.newBuilder().addAllStatRecords(
                          records(recordList1))
                          .build();

                responseObserver.onNext(stat);
            } else if (Long.parseLong(oid2) == entityOid) {
                // All records will be filtered out.
                final StatSnapshot stat = StatSnapshot.newBuilder().addAllStatRecords(
                          records(recordList2))
                          .build();

                responseObserver.onNext(stat);
            }

            responseObserver.onCompleted();
        }

        @Override
        public void getEntityStats(@Nonnull Stats.EntityStatsRequest request,
                                   @Nonnull StreamObserver<EntityStats> responseObserver) {
            request.getEntitiesList().forEach(entityOid -> {

                EntityStats statsForEntity = EntityStats.newBuilder()
                        .setOid(entityOid)
                        .build();
                responseObserver.onNext(statsForEntity);
            });
            responseObserver.onCompleted();
        }

        @Override
        public void getProjectedStats(ProjectedStatsRequest request,
                                      StreamObserver<ProjectedStatsResponse> responseObserver) {
            ProjectedStatsResponse response = ProjectedStatsResponse.newBuilder()
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getProjectedEntityStats(@Nonnull ProjectedStatsRequest request,
                                            @Nonnull StreamObserver<EntityStats> responseObserver) {
            request.getEntitiesList().forEach(entityOid ->
                responseObserver.onNext(EntityStats.newBuilder()
                        .setOid(entityOid)
                        .addStatSnapshots(StatSnapshot.newBuilder()
                                .build())
                        .build())
            );
            responseObserver.onCompleted();
        }
    }

    private List<StatRecord> records(final List<String> recordlist) {
        return recordlist.stream()
                         .map(name -> StatRecord.newBuilder().setName(name)
                              .setRelation(RelationType.COMMODITIES.getLiteral()).build())
                         .collect(Collectors.toList());
    }
}
