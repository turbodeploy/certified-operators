package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.input.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
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

    private TestClusterService testClusterService = spy(new TestClusterService());

    private TestGroupService groupServiceTest = spy(new TestGroupService());

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private GroupExpander groupExpander = Mockito.mock(GroupExpander.class);


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

    @Before
    public void setUp() throws IOException {
        GrpcTestServer testServer = GrpcTestServer.withServices(testStatsHistoryService,
                testClusterService, groupServiceTest);
        StatsHistoryServiceBlockingStub statsServiceRpc = StatsHistoryServiceGrpc.newBlockingStub(testServer.getChannel());
        groupExpander = Mockito.mock(GroupExpander.class);

        statsService = new StatsService(statsServiceRpc,
                repositoryApi, groupExpander, Clock.systemUTC());

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

//        doReturn(Optional.of(Arrays.asList(7L, 8L))).when(groupServiceTest).getMembers(
//                eq(Long.parseLong(oid1)));

        statsService.getStatsByEntityQuery(oid1, inputDto);

        ArgumentCaptor<EntityStatsRequest> requestCaptor =
                ArgumentCaptor.forClass(EntityStatsRequest.class);
        verify(testStatsHistoryService).getAveragedEntityStats(requestCaptor.capture(), any());
        assertThat(requestCaptor.getValue().getEntitiesList(), containsInAnyOrder(7L, 8L));
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
    }

    private class TestGroupService extends GroupServiceGrpc.GroupServiceImplBase {

        Optional<List<Long>> getMembers(@SuppressWarnings("unused") final long groupId) {
            return Optional.empty();
        }

        @Override
        public void getMembers(GetMembersRequest request,
                               StreamObserver<GetMembersResponse> responseObserver) {
            Optional<List<Long>> members = getMembers(request.getId());
            if (members.isPresent()) {
                responseObserver.onNext(GetMembersResponse.newBuilder()
                        .addAllMemberId(members.get())
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(io.grpc.Status.NOT_FOUND.asException());
            }
        }
    }


    private static class TestClusterService extends ClusterServiceImplBase {

        Optional<Cluster> getCluster(@SuppressWarnings("unused") final long clusterId) {
            return Optional.empty();
        }

        @Override
        public void getCluster(GetClusterRequest request,
                               StreamObserver<GetClusterResponse> responseObserver) {
            final GetClusterResponse.Builder resp = GetClusterResponse.newBuilder();
            getCluster(request.getClusterId()).ifPresent(resp::setCluster);
            responseObserver.onNext(resp.build());
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
