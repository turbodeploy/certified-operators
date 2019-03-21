package com.vmturbo.reports.component.data;

import javax.annotation.Nonnull;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;

/**
 * It contains external references to generate report data.
 */
public class ReportsDataContext {
    private final GroupServiceBlockingStub groupService;
    private final ReportDBDataWriter reportDataWriter;
    private final long realtimeTopologyContextId;

    public GroupServiceBlockingStub getGroupService() {
        return groupService;
    }

    public ReportDBDataWriter getReportDataWriter() {
        return reportDataWriter;
    }

    public SupplyChainServiceBlockingStub getSupplyChainRpcService() {
        return supplyChainRpcService;
    }

    public ActionsServiceBlockingStub getActionRpcService() {
        return actionRpcService;
    }

    private final SupplyChainServiceBlockingStub supplyChainRpcService;
    private final ActionsServiceBlockingStub actionRpcService;
    public ReportsDataContext(@Nonnull final GroupServiceBlockingStub groupService,
                              @Nonnull final ReportDBDataWriter reportDataWriter,
                              @Nonnull final Channel channel,
                              @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                              final long realtimeTopologyContextId) {
        this.groupService = groupService;
        this.reportDataWriter = reportDataWriter;
        this.supplyChainRpcService = SupplyChainServiceGrpc.newBlockingStub(channel);
        this.actionRpcService = actionsServiceBlockingStub;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    public long getRealTimeContextId() {
        return realtimeTopologyContextId;
    }
}
