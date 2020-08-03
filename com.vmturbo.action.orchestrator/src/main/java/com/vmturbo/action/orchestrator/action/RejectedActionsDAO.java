package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;

/**
 * Interface for communicating with rejected actions.
 */
public interface RejectedActionsDAO {

    /**
     * Persist rejection for rejected action.
     *
     * @param recommendationId the stable identifier for action
     * @param rejectedBy user who rejected the action
     * @param rejectedTime time when action was rejected
     * @param rejectingUserType type of user who rejected the action
     * @param relatedPolicies policies associated with the action
     * @throws ActionStoreOperationException if store operation failed
     */
    void persistRejectedAction(long recommendationId, @Nonnull String rejectedBy,
            @Nonnull LocalDateTime rejectedTime, @Nonnull String rejectingUserType,
            @Nonnull Collection<Long> relatedPolicies) throws ActionStoreOperationException;

    /**
     * Delete all actions with expired rejection.
     *
     * @param actionRejectionTTL rejection time to live for action (in minutes)
     */
    void removeExpiredRejectedActions(long actionRejectionTTL);

    /**
     * Get information about all rejected actions.
     *
     * @return list of rejected actions
     */
    @Nonnull
    List<RejectedActionInfo> getAllRejectedActions();

    /**
     * Remove rejections for all actions associated with this policy.
     *
     * @param policyId the policy id
     */
    void removeRejectionsForActionsAssociatedWithPolicy(long policyId);

}
