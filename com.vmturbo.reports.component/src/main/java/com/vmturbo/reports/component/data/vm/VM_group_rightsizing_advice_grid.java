package com.vmturbo.reports.component.data.vm;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

public class VM_group_rightsizing_advice_grid extends GroupsGenerator implements ReportTemplate {
    public VM_group_rightsizing_advice_grid(@Nonnull final GroupGeneratorDelegate delegate) {
        super(delegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        if (selectedGroup.isPresent()) {
            populateRightSize(context);
            return super.insertVMGroup(context, selectedGroup.get());
        }
        return Optional.empty();
    }

    // Get right size actions from AO and write them to DB
    private void populateRightSize(@Nonnull final ReportsDataContext context) throws DbException {
        final FilteredActionResponse response = context.getActionRpcService().getAllActions(
            FilteredActionRequest.newBuilder()
                .setTopologyContextId(context.getRealTimeContextId())
                .setFilter(ActionQueryFilter
                    .newBuilder()
                    .setVisible(true)
                    .addStates(ActionState.READY)
                    .addTypes(ActionType.RESIZE)
                    .build())
                .build());
        final List<ActionSpec> actionSpecList = response.getActionsList().stream()
            .filter(ActionOrchestratorAction::hasActionSpec)
            .map(ActionOrchestratorAction::getActionSpec)
            // Check if we need to describe the action as a "remove limit" instead of regular resize.
            .filter(actionSpec -> actionSpec.getRecommendation()
                .getInfo().getResize().getCommodityAttribute() != CommodityAttribute.LIMIT)
            .collect(Collectors.toList());
        context.getReportDataWriter().insertRightSizeActions(actionSpecList);
    }
}
