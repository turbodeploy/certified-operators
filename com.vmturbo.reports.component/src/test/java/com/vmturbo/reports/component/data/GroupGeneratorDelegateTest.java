package com.vmturbo.reports.component.data;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import javaslang.Tuple;

import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.reports.component.data.ReportDataUtils.EntitiesTableGeneratedId;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit test to cover all the cases of generating group relationship data for report in {@link GroupGeneratorDelegateTest}.
 */
public class GroupGeneratorDelegateTest {

    private GroupGeneratorDelegate groupGeneratorDelegate;
    private ReportsDataContext context;
    private GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());
    private SupplyChainServiceMole supplyChainServiceMole = Mockito.spy(new SupplyChainServiceMole());
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceMole, supplyChainServiceMole);
    private ReportDBDataWriter reportDBDataWriter = mock(ReportDBDataWriter.class);
    private EntitiesTableGeneratedId results = mock(EntitiesTableGeneratedId.class);
    private EntitiesTableGeneratedId newResults = mock(EntitiesTableGeneratedId.class);
    private final GetSupplyChainResponse response = GetSupplyChainResponse
        .newBuilder()
        .setSupplyChain(SupplyChain.newBuilder()
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                .putMembersByState(EntityState.POWERED_ON_VALUE,
                    MemberList.newBuilder().addMemberOids(1L)
                        .build()).build()).build()).build();

    @Before
    public void init() throws Exception {
        groupGeneratorDelegate = new GroupGeneratorDelegate(60, 100000);
        context = mock(ReportsDataContext.class);
        Mockito.when(groupServiceMole.getGroup(any()))
            .thenReturn(GetGroupResponse.newBuilder()
                .setGroup(Grouping.newBuilder()
                    .setDefinition(GroupDefinition.newBuilder().setType(GroupType.COMPUTE_HOST_CLUSTER)))
                .build());

        Mockito.when(supplyChainServiceMole.getSupplyChain(any()))
            .thenReturn(response);
        when(context.getGroupService()).thenReturn(GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        when(context.getReportDataWriter()).thenReturn(reportDBDataWriter);
        when(reportDBDataWriter.insertGroupIntoEntitiesTable(anyList(), any())).thenReturn(results);
        when(reportDBDataWriter.insertGroup(any(), any())).thenReturn(Tuple.of(results, Optional.of("1")));
        // context.getReportDataWriter().insertEntityAssns
        when(reportDBDataWriter.insertEntityAssns(results)).thenReturn(newResults);

        when(context.getSupplyChainRpcService()).thenReturn(SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        Grouping group = Grouping.newBuilder().build();
        Map<Grouping, Long> groupToPK = ImmutableMap.of(group, 1L);
        when(newResults.getGroupToPK()).thenReturn(groupToPK);
        Mockito.when(groupServiceMole.createGroup(any()))
            .thenReturn(CreateGroupResponse.newBuilder().setGroup(group).build());
    }

    @After
    public void cleanup() {
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRetryRepositoryLogicInvalidArgment1() throws DbException {
        new GroupGeneratorDelegate(1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRetryRepositoryLogicInvalidArgment2() throws DbException {
        new GroupGeneratorDelegate(100, 10);
    }

    /**
     * Verify if the retries <= maxConnectRetryCount (2), it will retry again.
     */
    @Test
    public void testRetryRepositoryLogicWithGrpcException() throws DbException {
        GroupGeneratorDelegate groupGeneratorDelegate = new GroupGeneratorDelegate(2, 1001);
        Mockito.when(supplyChainServiceMole.getSupplyChain(any()))
            .thenThrow(new StatusRuntimeException(Status.INTERNAL)).
            thenThrow(new StatusRuntimeException(Status.INTERNAL)).
            thenReturn(response);
        groupGeneratorDelegate.insertVMClusterRelationships(context);
        verify(context).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter,times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    /**
     * Verify if the retried > maxConnectRetryCount (2 in this case), a RuntimeException will throw.
     */
    @Test(expected = RuntimeException.class)
    public void testRetryRepositoryLogicWithGrpcExceptionBeyondFix() throws DbException {
        GroupGeneratorDelegate groupGeneratorDelegate = new GroupGeneratorDelegate(2, 1001);
        Mockito.when(supplyChainServiceMole.getSupplyChain(any()))
            .thenThrow(new StatusRuntimeException(Status.INTERNAL)).
            thenThrow(new StatusRuntimeException(Status.INTERNAL)).
            thenThrow(new StatusRuntimeException(Status.INTERNAL)).
            thenReturn(response);
        groupGeneratorDelegate.insertVMClusterRelationships(context);
    }

    @Test
    public void testInsertVMClusterRelationships() throws DbException {
        groupGeneratorDelegate.insertVMClusterRelationships(context);
        verify(context).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter,times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertVMGroupRelationships() throws DbException {
        groupGeneratorDelegate.insertVMGroupRelationships(context, 1L);
        verify(context, times(2)).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter, times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertPMClusterRelationships() throws DbException {
        groupGeneratorDelegate.insertPMClusterRelationships(context);
        verify(context).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter, times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertSTClusterRelationships() throws DbException {
        groupGeneratorDelegate.insertStorageClusterRelationships(context);
        verify(context).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter , times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertPMGroupRelationships() throws DbException {
        groupGeneratorDelegate.insertPMGroupRelationships(context, 1L);
        verify(context, times(2)).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter , times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertPMGroupAndVMRelationships() throws DbException {
        groupGeneratorDelegate.insertPMGroupAndVMRelationships(context, 1L);
        verify(context, times(4)).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter , times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertPMVMsRelationships() throws DbException {
        groupGeneratorDelegate.insertPMVMsRelationships(context, 1L);
        verify(context, times(2)).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(7)).getReportDataWriter();
        verify(reportDBDataWriter , times(2)).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

}
