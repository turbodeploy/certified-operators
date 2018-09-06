package com.vmturbo.action.orchestrator.workflow.store;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.OrchestratorType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;

/**
 * Store for Workflow items.
 * Implementations may choose whether to provide persistence.
 **/
public interface WorkflowStore {

    /**
     * Store the information about a each Workflow in the given list. The workflow is identified
     * by the unique OID of the target from which the workflow was discovere and by a 'name' -
     * the external name by which the workflow is known on the target.
     *
     * If a Workflow exists, then it is updated (if changed). If a Workflow doesn't yet exist then
     * it will be added.
     *
     * Any workflow previously stored for this target but not in the new list is removed.
     *
     * @param targetId the target from which these workflows were discovered
     * @param worflowInfos a list of WorkflowInfo protobufs describing the workflows to be stored.
     *                     Previously exising workflows will be updated. New workflows will be
     *                     added. Old workflows not on this list will be removed.
     * @throws WorkflowStoreException if there is a problem saving either the OID of a workflow
     * or the WorkflowInfo for the workflow.
     */
    void persistWorkflows(long targetId, List<WorkflowInfo> worflowInfos)
            throws WorkflowStoreException;

    /**
     * Return the workflows that match the given 'orchestratorTypeFilter'. If the filter == null then
     * all workflows will be returned.
     *
     * Note that the filter is not yet implemented, and not used by the UI. It should probably be
     * removed from the request.
     *
     * @param orchestratorTypeFilter what type of OrchestrationTarget to include; or 'all' if null
     * @return all the {@link Workflow} protobufs whose type matches the 'orchestratorTypeFilter'
     * @throws WorkflowStoreException if there is any error fetching from the backing store
     */
    @Nonnull
    Set<Workflow> fetchWorkflows(@Nullable OrchestratorType orchestratorTypeFilter)
            throws WorkflowStoreException;
}
