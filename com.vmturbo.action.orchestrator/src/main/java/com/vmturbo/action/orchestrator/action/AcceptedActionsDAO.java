package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;

/**
 * Interface for communicating with accepted/approved actions.
 */
public interface AcceptedActionsDAO {

    /**
     * Persist acceptance for accepted/approved action.
     *
     * @param recommendationId the stable identifier for action
     * @param lastGeneratedTime time when action was recommended
     * @param acceptedBy user who accepted the action
     * @param acceptedTime time when action was accepted
     * @param acceptorType acceptor type (Turbo user or user from Orchestration platform)
     * @param relatedPolicies policies associated with the action
     * @throws ActionStoreOperationException if store operation failed
     */
    void persistAcceptedAction(long recommendationId, @Nonnull LocalDateTime lastGeneratedTime,
            @Nonnull String acceptedBy, @Nonnull LocalDateTime acceptedTime,
            @Nonnull String acceptorType, @Nonnull Collection<Long> relatedPolicies)
            throws ActionStoreOperationException;

    /**
     * Delete acceptance for actions. (e.g. if action was successfully executed)
     *
     * @param recommendationId the action stable identifier
     */
    void deleteAcceptedAction(long recommendationId);

    /**
     * Delete all actions with expired acceptance (i.e accepted action was not recommended by
     * market during actionAcceptanceTTL from latest recommendation time).
     *
     * @param actionAcceptanceTTL acceptance time to live for action (in minutes)
     */
    void removeExpiredActions(long actionAcceptanceTTL);

    /**
     * Get information about certain accepted actions.
     *
     * @param recommendationIds the list of accepted actions recommendationIds
     * @return list of accepted actions
     */
    @Nonnull
    List<AcceptedActionInfo> getAcceptedActions(@Nonnull Collection<Long> recommendationIds);

    /**
     * Get information about all accepted actions.
     *
     * @return list of accepted actions
     */
    @Nonnull
    List<AcceptedActionInfo> getAllAcceptedActions();

    /**
     * Get acceptors for all accepted actions in store.
     *
     * @return map of recommendationId -> acceptedBy
     */
    @Nonnull
    Map<Long, String> getAcceptorsForAllActions();

    /**
     * Update latest recommendation time for actions.
     *
     * @param actionsRecommendationIds actions which latest recommendation time
     * should be updated
     * @throws ActionStoreOperationException if store operation failed
     */
    void updateLatestRecommendationTime(@Nonnull Collection<Long> actionsRecommendationIds)
            throws ActionStoreOperationException;

    /**
     * Remove acceptance for all actions associated with this policy.
     *
     * @param policyId the policy id
     */
    void removeAcceptanceForActionsAssociatedWithPolicy(long policyId);
}
