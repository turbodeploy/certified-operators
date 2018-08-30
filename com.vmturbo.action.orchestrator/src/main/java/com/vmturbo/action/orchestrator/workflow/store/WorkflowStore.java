package com.vmturbo.action.orchestrator.workflow.store;

import java.util.List;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.identity.store.IdentityStoreException;

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
     * @throws PersistWorkflowException if there is a problem saving either the OID of a workflow
     * or the WorkflowInfo for the workflow.
     */
    void persistWorkflows(long targetId, List<WorkflowDTO.WorkflowInfo> worflowInfos)
            throws PersistWorkflowException;
}
