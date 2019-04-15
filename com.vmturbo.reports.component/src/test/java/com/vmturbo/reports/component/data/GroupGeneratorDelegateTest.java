package com.vmturbo.reports.component.data;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

import javaslang.Tuple;

import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
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

    @Before
    public void init() throws Exception {
        groupGeneratorDelegate = new GroupGeneratorDelegate();
        context = mock(ReportsDataContext.class);
        Mockito.when(groupServiceMole.getGroup(any()))
            .thenReturn(GetGroupResponse.newBuilder()
                .setGroup(Group.newBuilder()
                    .setType(Group.Type.CLUSTER))
                .build());

        final GetSupplyChainResponse response = GetSupplyChainResponse
            .newBuilder()
            .setSupplyChain(SupplyChain.newBuilder()
                .addSupplyChainNodes(SupplyChainNode.newBuilder()
                    .putMembersByState(EntityState.POWERED_ON_VALUE,
                        MemberList.newBuilder().addMemberOids(1L)
                        .build()).build()).build()).build();

        Mockito.when(supplyChainServiceMole.getSupplyChain(any()))
            .thenReturn(response);
        when(context.getGroupService()).thenReturn(GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        when(context.getReportDataWriter()).thenReturn(reportDBDataWriter);
        when(reportDBDataWriter.insertGroupIntoEntitiesTable(anyList(), any())).thenReturn(results);
        when(reportDBDataWriter.insertGroup(any(), any())).thenReturn(Tuple.of(results, Optional.of("1")));
        // context.getReportDataWriter().insertEntityAssns
        when(reportDBDataWriter.insertEntityAssns(results)).thenReturn(newResults);

        when(context.getSupplyChainRpcService()).thenReturn(SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        Group group = Group.newBuilder().build();
        Map<Group, Long> groupToPK = ImmutableMap.of(group, 1L);
        when(newResults.getGroupToPK()).thenReturn(groupToPK);
        Mockito.when(groupServiceMole.createTempGroup(any()))
            .thenReturn(CreateTempGroupResponse.newBuilder().setGroup(group).build());
    }

    @After
    public void cleanup() {
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
