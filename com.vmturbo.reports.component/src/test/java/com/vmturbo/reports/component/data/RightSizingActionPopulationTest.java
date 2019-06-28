package com.vmturbo.reports.component.data;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.reports.component.data.vm.Daily_vm_rightsizing_advice_grid;

public class RightSizingActionPopulationTest {

    private GroupGeneratorDelegate groupGeneratorDelegate;
    private ReportsDataContext context;
    private ActionsServiceMole actionsServiceMole = Mockito.spy(new ActionsServiceMole());
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceMole);
    private ReportDBDataWriter reportDBDataWriter = mock(ReportDBDataWriter.class);

    @Before
    public void init() throws Exception {
        groupGeneratorDelegate = new GroupGeneratorDelegate(60, 100000);
        context = mock(ReportsDataContext.class);
        when(context.getReportDataWriter()).thenReturn(reportDBDataWriter);
        when(context.getActionRpcService()).thenReturn(ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        final FilteredActionResponse filteredActionResponse = FilteredActionResponse.newBuilder()
            .setPaginationResponse(PaginationResponse.newBuilder().setNextCursor("10").build()).build();
        Mockito.when(actionsServiceMole.getAllActions(any()))
            .thenReturn(filteredActionResponse)
            .thenReturn(filteredActionResponse)
            .thenReturn(FilteredActionResponse.getDefaultInstance());
    }

    // verify it will retrieve and persist all actions in chunk.
    @Test
    public void testPopulateRightSize() throws Exception {
        Daily_vm_rightsizing_advice_grid report = new Daily_vm_rightsizing_advice_grid(groupGeneratorDelegate);
        report.populateRightSize(context);
        verify(reportDBDataWriter).cleanUpRightSizeActions();
        verify(actionsServiceMole, times(3)).getAllActions(any());
        verify(reportDBDataWriter, times(3)).insertRightSizeActions(anyList());
    }
}
