package com.vmturbo.reports.component.data;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.reports.component.data.ReportDataUtils.Results;
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
    private Results results = mock(Results.class);
    private Results newResults = mock(Results.class);

    @Before
    public void init() throws Exception {
        groupGeneratorDelegate = new GroupGeneratorDelegate();
        context = mock(ReportsDataContext.class);
        Mockito.when(groupServiceMole.getGroup(GroupID.newBuilder()
            .setId(123L)
            .build()))
            .thenReturn(GetGroupResponse.newBuilder()
                .setGroup(Group.newBuilder()
                    .setType(Group.Type.CLUSTER))
                .build());

        final GetSupplyChainResponse response = GetSupplyChainResponse
            .newBuilder()
            .setSupplyChain(SupplyChain.newBuilder()
                .addSupplyChainNodes(SupplyChainNode.newBuilder().build())
                .build())
            .build();
        Mockito.when(supplyChainServiceMole.getSupplyChain(any()))
            .thenReturn(response);
        when(context.getGroupService()).thenReturn(GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        when(context.getReportDataWriter()).thenReturn(reportDBDataWriter);
        when(reportDBDataWriter.insertGroups(anyList(), any())).thenReturn(results);
        // context.getReportDataWriter().insertEntityAssns
        when(reportDBDataWriter.insertEntityAssns(results)).thenReturn(newResults);

        when(context.getSupplyChainRpcService()).thenReturn(SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        Group group = Group.newBuilder().build();
        Map<Group, Long> groupToPK = ImmutableMap.of(group, 1L);
        when(newResults.getGroupToPK()).thenReturn(groupToPK);
    }

    @After
    public void cleanup() {
    }

    @Test
    public void testInsertVMGroupRelationships() throws DbException {
        groupGeneratorDelegate.insertVMGroupRelationships(context);
        verify(context).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(6)).getReportDataWriter();
        verify(reportDBDataWriter).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertPMGroupRelationships() throws DbException {
        groupGeneratorDelegate.insertPMGroupRelationships(context);
        verify(context).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(6)).getReportDataWriter();
        verify(reportDBDataWriter).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }

    @Test
    public void testInsertSTGroupRelationships() throws DbException {
        groupGeneratorDelegate.insertStorageGroupRelationships(context);
        verify(context).getGroupService();
        verify(results).getGroupToPK();
        verify(context, times(6)).getReportDataWriter();
        verify(reportDBDataWriter).cleanUpEntity_Assns(anyList());
        verify(reportDBDataWriter).insertEntityAssns(any());
        verify(reportDBDataWriter).insertEntityAssnsMembersEntities(anyMap());
        verify(reportDBDataWriter).insertEntityAttrs(anyList(), any());
    }
}
