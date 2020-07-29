package com.vmturbo.action.orchestrator.workflow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowFilter;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;

/**
 * Diagnostics dumper/imported for workflows.
 */
public class WorkflowDiagnostics implements DiagsRestorable {

    private final WorkflowStore store;
    private final Logger logger = LogManager.getLogger(getClass());
    private final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();

    /**
     * Constructs workflow diagnostics.
     *
     * @param store workflow store to use to get and persist workflows.
     */
    public WorkflowDiagnostics(@Nonnull WorkflowStore store) {
        this.store = store;
    }

    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        logger.info("Restoring {} workflows from diags", collectedDiags::size);
        final List<Workflow> workflows = new ArrayList<>(collectedDiags.size());
        for (String diags : collectedDiags) {
            workflows.add(gson.fromJson(diags, Workflow.class));
        }
        final Map<Long, List<Workflow>> mapByTarget = new HashMap<>(workflows.stream()
                .collect(Collectors.groupingBy(
                        workflow -> workflow.getWorkflowInfo().getTargetId())));
        try {
            for (Long targetId : getAllTargets()) {
                mapByTarget.computeIfAbsent(targetId, key -> Collections.emptyList());
            }
        } catch (WorkflowStoreException e) {
            throw new DiagnosticsException("Failed fetching all the targets for existing workflows."
                    + " Try removing all the workflows manually before import", e);
        }
        final Collection<Long> targetIds = new ArrayList<>();
        for (Entry<Long, List<Workflow>> entry : mapByTarget.entrySet()) {
            final long targetId = entry.getKey();
            final List<Workflow> targetWorkflows = entry.getValue();
            logger.debug("Restoring {} workflows for target {}...", targetWorkflows::size,
                    () -> targetId);
            try {
                store.persistWorkflows(targetId,
                        Lists.transform(targetWorkflows, Workflow::getWorkflowInfo));
            } catch (WorkflowStoreException e) {
                logger.error(
                        String.format("Failed importing workflows for target %d: [%s]", targetId,
                                targetWorkflows.stream()
                                        .map(workflow -> workflow.getId() + "("
                                                + workflow.getWorkflowInfo().getName() + ")")
                                        .collect(Collectors.joining(","))), e);
                targetIds.add(targetId);
            }
        }
        logger.info("Finished importing {} workflows", collectedDiags.size());
        if (!targetIds.isEmpty()) {
            throw new DiagnosticsException("Failed loading workflows from targets " + targetIds);
        }
    }

    @Nonnull
    private Set<Long> getAllTargets() throws WorkflowStoreException {
        return store.fetchWorkflows(new WorkflowFilter(Collections.emptyList()))
                .stream()
                .map(Workflow::getWorkflowInfo)
                .map(WorkflowInfo::getTargetId)
                .collect(Collectors.toSet());
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final List<WorkflowDTO.Workflow> workflows;
        try {
            logger.debug("Loading all workflows from store");
            workflows = new ArrayList<>(
                    store.fetchWorkflows(new WorkflowFilter(Collections.emptyList())));
        } catch (WorkflowStoreException e) {
            throw new DiagnosticsException("Failed to fetch workflows from store", e);
        }
        logger.debug("Saving all {} workflows to diagnostics", workflows::size);
        for (Workflow workflow : workflows) {
            logger.trace("Exporting workflow {} ({})...", workflow::getId,
                    () -> workflow.getWorkflowInfo().getName());
            appender.appendString(gson.toJson(workflow));
        }
        logger.info("Successfully exported {} workflows to diagnostics", workflows::size);
    }

    @Nonnull
    @Override
    public String getFileName() {
        return "workflows";
    }
}
