package com.vmturbo.action.orchestrator.action;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Interface for persisting audited actions.
 */
public interface AuditActionsPersistenceManager {

    /**
     * Persist an action that was sent for audit.
     *
     * @param actionInfos information about audited actions contains action identifier,
     * workflow identifier and cleared timestamp. If action is recommended by market then
     * cleared timestamp = null, if action is cleared then cleared timestamp = time when action
     * was cleared first time.
     * @throws ActionStoreOperationException if store operation failed
     */
    void persistActions(@Nonnull Collection<AuditedActionInfo> actionInfos)
            throws ActionStoreOperationException;

    /**
     * Remove audited actions by action oid and workflow oid pairs. An example usecase is when
     * the cleared criteria of the audits was satisfied so we need to remove them.
     *
     * @param actionsToRemove pairs of audited actions with associated workflows that we
     * need to remove. The first value is the action oid, the second is the workflow oid.
     * @throws ActionStoreOperationException if store operation failed
     */
    void removeActionWorkflows(@Nonnull Collection<Pair<Long, Long>> actionsToRemove)
            throws ActionStoreOperationException;

    /**
     * Remove audited actions by recommendation oid because those actions with the provided
     * recommendation oids completed execution. If there are multiple workflow oids for the
     * action oid, they will all be removed.
     *
     * @param recommendationOids the recommendation oids of the action to remove from the book
     *                           keeping database.
     * @throws ActionStoreOperationException if the store operation failed.
     */
    void removeActionsByRecommendationOid(@Nonnull Collection<Long> recommendationOids)
        throws ActionStoreOperationException;

    /**
     * Get all actions that we already persisted.
     * Contains actions recommended by market or actions that have been cleared, but haven't been
     * removed due to meeting the cleared criteria yet.
     *
     * @return collection of actions with info sending for audit
     */
    Collection<AuditedActionInfo> getActions();

    /**
     * Delete audited actions related to certain workflow.
     * For example, this is called when the customer removes a target so we need to remove all data
     * related to the discovered workflows like audited actions.
     *
     * @param workflowId the ID of the workflow
     */
    void deleteActionsRelatedToWorkflow(long workflowId);
}
