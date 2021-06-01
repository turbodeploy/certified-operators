package com.vmturbo.action.orchestrator.workflow.store;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowFilter;
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
     * Return the workflows that match the given filter. If there are no restrictions in the filter
     * then all workflows will be returned.
     *
     * @param workflowFilter workflow filter contains different restrictions
     * @return all the {@link Workflow} protobufs matches the filter's conditions
     * @throws WorkflowStoreException if there is any error fetching from the backing store
     */
    @Nonnull
    Set<Workflow> fetchWorkflows(@Nonnull WorkflowFilter workflowFilter)
            throws WorkflowStoreException;

    /**
     * Fetch an Optional containing the {@link Workflow} corresponding to the given id.
     *
     * If not found then return Optional.empty()
     *
     * @param workflowId the unique ID of the workflow to fetch
     * @return an Optional containing the Workflow corresponding to the given id, or
     * Optional.empty() if not found.
     * @throws WorkflowStoreException there's an error in the underlying workflow store
     */
    @Nonnull
    Optional<Workflow> fetchWorkflow(long workflowId) throws WorkflowStoreException;
}
