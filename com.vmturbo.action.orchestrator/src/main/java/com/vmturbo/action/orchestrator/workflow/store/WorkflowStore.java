package com.vmturbo.action.orchestrator.workflow.store;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowFilter;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;

/**
 * Store for Workflow items.
 * Implementations may choose whether to provide persistence.
 */
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
     * Create a single user defined workflow. These differ from persistWorkflows because they are not
     * discovered by probes. Any user-created workflows (e.g. webhook workflows) created by
     * insertWorkflow will not be removed by persistWorkflows.
     *
     * @param workflowInfo the info of the user-created workflow.
     * @return the id of the created workflow.
     */
    long insertWorkflow(WorkflowInfo workflowInfo) throws WorkflowStoreException ;

    /**
     * Updates a user defined workflow. Updates through persistWorkflows differs because persistWorkflows
     * are discovered by probes.
     *
     * @param workflowId the id of the workflow to update.
     * @param workflowInfo the details of the workflow that will replace the existing details.
     */
    void updateWorkflow(long workflowId, WorkflowInfo workflowInfo) throws WorkflowStoreException ;

    /**
     * Deletes a user created workflow. Probe discovered workflows cannot be deleted. Probe
     * discovered workflows will be deleted by persistWorkflows when they are no longer submitted
     * by the probe.
     *
     * @param workflowId the id of a user created workflow.
     */
    void deleteWorkflow(long workflowId) throws WorkflowStoreException ;
    
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

    /**
     * Get a {@link Workflow} by display name. If not found, then return Optional.empty().
     *
     * @param displayName the input display name used to search the workflow for.
     * @return an Optional containing the Workflow corresponding to the given display name,
     *     or Optional.empty() if not found.
     * @throws WorkflowStoreException if there's an error while getting the workflow display name.
     */
    @Nonnull
    Optional<Workflow> getWorkflowByDisplayName(String displayName) throws WorkflowStoreException;

}
