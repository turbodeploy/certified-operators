package com.vmturbo.reports.component.data.vm;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert report data to vmtdb for Daily_vm_rightsizing_advice_grid template .
 */
public class Daily_vm_rightsizing_advice_grid extends GroupsGenerator implements ReportTemplate {

    private static final Logger logger = LogManager.getLogger();
    private static final int LIMIT = 1000;

    public Daily_vm_rightsizing_advice_grid(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        super(groupGeneratorDelegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        super.insertVMGroups(context);
        populateRightSize(context);
        return Optional.empty();
    }

    // Get right size actions from AO and write them to DB
    public void populateRightSize(@Nonnull final ReportsDataContext context) throws DbException {
        context.getReportDataWriter().cleanUpRightSizeActions();
        processPaginatedAction(context);
    }

    /**
     * Retrieve and persist all actions in chunk.
     */
    private void processPaginatedAction(@Nonnull final ReportsDataContext context) {
        try {
            final FilteredActionRequest.Builder requestBuilder = FilteredActionRequest.newBuilder()
                .setTopologyContextId(context.getRealTimeContextId())
                .setPaginationParams(PaginationParameters.newBuilder().setLimit(LIMIT).build())
                .setFilter(ActionQueryFilter
                    .newBuilder()
                    .setVisible(true)
                    .addStates(ActionState.READY)
                    .addTypes(ActionType.RESIZE)
                    .build());
            String nextCursor = "";
            do {
                // pagination cursor doesn't accept empty value, so avoid setting it.
                if (!StringUtils.isEmpty(nextCursor)) {
                    requestBuilder.getPaginationParamsBuilder().setCursor(nextCursor);
                }
                final FilteredActionResponse response = context.getActionRpcService()
                    .getAllActions(requestBuilder.build());
                final List<ActionSpec> actionSpecList = response.getActionsList().stream()
                    .filter(ActionOrchestratorAction::hasActionSpec)
                    .map(ActionOrchestratorAction::getActionSpec)
                    // Check if we need to describe the action as a "remove limit" instead of regular resize.
                    .filter(actionSpec -> actionSpec.getRecommendation()
                        .getInfo().getResize().getCommodityAttribute() != CommodityAttribute.LIMIT)
                    .collect(Collectors.toList());
                context.getReportDataWriter().insertRightSizeActions(actionSpecList);
                nextCursor = PaginationProtoUtil.getNextCursor(response.getPaginationResponse()).orElse("");
            } while (!StringUtils.isEmpty(nextCursor));
        } catch (DbException e) {
            logger.error("Failed to persist actions", e);
            return;
        }
    }
}
