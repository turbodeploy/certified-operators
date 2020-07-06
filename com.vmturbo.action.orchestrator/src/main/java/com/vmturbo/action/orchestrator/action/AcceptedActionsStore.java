package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.AcceptedActions.ACCEPTED_ACTIONS;
import static com.vmturbo.action.orchestrator.db.tables.AcceptedActionsPolicies.ACCEPTED_ACTIONS_POLICIES;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.TableRecord;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.tables.records.AcceptedActionsPoliciesRecord;
import com.vmturbo.action.orchestrator.db.tables.records.AcceptedActionsRecord;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;

/**
 * The {@link AcceptedActionsStore} class is used for CRUD operations on accepted/approved actions.
 */
public class AcceptedActionsStore implements AcceptedActionsDAO {

    /**
     * Database access context.
     */
    private final DSLContext dslContext;

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor of {@link AcceptedActionsStore}.
     *
     * @param dslContext database access context
     */
    public AcceptedActionsStore(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    @Override
    public void persistAcceptedAction(long recommendationId,
            @Nonnull LocalDateTime actionLatestRecommendationTime, @Nonnull String acceptedBy,
            @Nonnull LocalDateTime acceptedTime, @Nonnull String acceptorType,
            @Nonnull Collection<Long> relatedPolicies) throws ActionStoreOperationException {

        final Collection<TableRecord<?>> inserts = new ArrayList<>(
                createAcceptedAction(recommendationId, actionLatestRecommendationTime, acceptedBy,
                        acceptedTime, acceptorType, relatedPolicies));
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final Result<AcceptedActionsRecord> existingAcceptanceForAction =
                        context.selectFrom(ACCEPTED_ACTIONS)
                                .where(ACCEPTED_ACTIONS.RECOMMENDATION_ID.eq(recommendationId))
                                .fetch();
                if (!existingAcceptanceForAction.isEmpty()) {
                    throw new ActionStoreOperationException(
                            "Action " + recommendationId + " has been already accepted by "
                                    + existingAcceptanceForAction.stream()
                                    .findFirst()
                                    .get()
                                    .getAcceptedBy());
                }
                context.batchInsert(inserts).execute();
            });
        } catch (DataAccessException ex) {
            if (ex.getCause() instanceof ActionStoreOperationException) {
                throw (ActionStoreOperationException)ex.getCause();
            } else {
                throw ex;
            }
        }
    }

    @Override
    public void deleteAcceptedAction(long recommendationId) {
        dslContext.transaction(configuration -> {
            final DSLContext context = DSL.using(configuration);
            context.deleteFrom(ACCEPTED_ACTIONS)
                    .where(ACCEPTED_ACTIONS.RECOMMENDATION_ID.eq(recommendationId))
                    .execute();
            logger.info("Acceptance was removed for action {}", recommendationId);
        });
    }

    @Override
    public void removeExpiredActions(final long actionAcceptanceTTLinMinutes) {
        dslContext.transaction(
                configuration -> deleteActionWithExpiredAcceptance(actionAcceptanceTTLinMinutes,
                        configuration));
    }

    private void deleteActionWithExpiredAcceptance(long actionAcceptanceTTLinMinutes,
            @Nonnull Configuration configuration) {
        final DSLContext context = DSL.using(configuration);
        final Result<AcceptedActionsRecord> acceptedActions =
                context.selectFrom(ACCEPTED_ACTIONS).fetch();
        final Map<Long, AcceptedActionsRecord> actionsToDelete = acceptedActions.stream()
                .filter(action ->
                        Duration.between(action.getLatestRecommendationTime(), LocalDateTime.now())
                                .toMinutes() > actionAcceptanceTTLinMinutes)
                .collect(Collectors.toMap(AcceptedActionsRecord::getRecommendationId, action -> action));
        if (!actionsToDelete.isEmpty()) {
            context.deleteFrom(ACCEPTED_ACTIONS)
                    .where(ACCEPTED_ACTIONS.RECOMMENDATION_ID.in(actionsToDelete.keySet()))
                    .execute();
            final List<String> deletedActionsInfo = actionsToDelete.values()
                    .stream()
                    .map(el -> "(" + el.getRecommendationId() + ", " + el.getLatestRecommendationTime()
                            + ')')
                    .collect(Collectors.toList());
            logger.info("Deleted actions with expired acceptance (with information "
                            + "about latest recommendation time): {}",
                    StringUtils.join(deletedActionsInfo, ","));
        } else {
            logger.debug("There are no actions with expired acceptance.");
        }
    }

    @Override
    @Nonnull
    public List<AcceptedActionInfo> getAcceptedActions(@Nonnull Collection<Long> recommendationIds) {
        return dslContext.transactionResult(
                configuration -> selectAcceptedActions(recommendationIds, configuration));
    }

    @Nonnull
    @Override
    public List<AcceptedActionInfo> getAllAcceptedActions() {
        return dslContext.transactionResult(
                configuration -> selectAcceptedActions(Collections.emptyList(), configuration));
    }

    @Nonnull
    private List<AcceptedActionInfo> selectAcceptedActions(@Nonnull Collection<Long> recommendationIds,
            @Nonnull Configuration configuration) {
        final DSLContext context = DSL.using(configuration);
        final List<Condition> conditions = new ArrayList<>();
        if (!recommendationIds.isEmpty()) {
            conditions.add(ACCEPTED_ACTIONS.RECOMMENDATION_ID.in(recommendationIds));
        }
        final Result<AcceptedActionsRecord> acceptedActionsRecords =
                context.selectFrom(ACCEPTED_ACTIONS).where(conditions).fetch();
        final List<Long> acceptedRecommendationIds = acceptedActionsRecords.stream()
                .map(AcceptedActionsRecord::getRecommendationId)
                .collect(Collectors.toList());
        final Map<Long, Collection<Long>> actionToPoliciesMap =
                populateAssociatedPolicies(context, acceptedRecommendationIds);
        return convertToAcceptedActionInfo(acceptedActionsRecords, actionToPoliciesMap);
    }

    @Override
    @Nonnull
    public Map<Long, String> getAcceptorsForAllActions() {
        return dslContext.transactionResult(configuration -> {
            final DSLContext context = DSL.using(configuration);
            return context.selectFrom(ACCEPTED_ACTIONS)
                    .fetch()
                    .stream()
                    .collect(Collectors.toMap(AcceptedActionsRecord::getRecommendationId,
                            AcceptedActionsRecord::getAcceptedBy));
        });
    }

    @Override
    public void updateLatestRecommendationTime(@Nonnull Collection<Long> actionsRecommendationIds)
            throws ActionStoreOperationException {
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                context.update(ACCEPTED_ACTIONS)
                        .set(ACCEPTED_ACTIONS.LATEST_RECOMMENDATION_TIME, LocalDateTime.now())
                        .where(ACCEPTED_ACTIONS.RECOMMENDATION_ID.in(actionsRecommendationIds))
                        .execute();
            });
        } catch (DataAccessException exception) {
            throw new ActionStoreOperationException(
                    "Failed to update latest recommendation time for recommended accepted actions "
                            + StringUtils.join(actionsRecommendationIds, ","));
        }
    }

    @Override
    public void removeAcceptanceForActionsAssociatedWithPolicy(long policyId) {
        dslContext.transaction(
                configuration -> deleteActionsAssociatedWithPolicy(policyId, configuration));
    }

    private void deleteActionsAssociatedWithPolicy(long policyId,
            @Nonnull Configuration configuration) {
        final DSLContext context = DSL.using(configuration);
        final Result<AcceptedActionsPoliciesRecord> actionsPolicyRecords =
                dslContext.selectFrom(ACCEPTED_ACTIONS_POLICIES)
                        .where(ACCEPTED_ACTIONS_POLICIES.POLICY_ID.eq(policyId))
                        .fetch();
        if (!actionsPolicyRecords.isEmpty()) {
            final Set<Long> actionsToDelete = actionsPolicyRecords.stream()
                    .map(AcceptedActionsPoliciesRecord::getRecommendationId)
                    .collect(Collectors.toSet());
            context.deleteFrom(ACCEPTED_ACTIONS)
                    .where(ACCEPTED_ACTIONS.RECOMMENDATION_ID.in(actionsToDelete))
                    .execute();
            logger.info("Removed acceptance for actions `{}` associated with policy {}",
                    StringUtils.join(actionsToDelete, ","), policyId);
        } else {
            logger.debug(
                    "There are no accepted actions associated with policy {} for deleting.",
                    policyId);
        }
    }

    @Nonnull
    private static Collection<TableRecord<?>> createAcceptedAction(long recommendationId,
            @Nonnull LocalDateTime actionLatestRecommendationTime, @Nonnull String acceptedBy,
            @Nonnull LocalDateTime acceptedTime, @Nonnull String acceptorType,
            @Nonnull Collection<Long> relatedPolicies) {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        final AcceptedActionsRecord acceptedActionRecord =
                new AcceptedActionsRecord(recommendationId, acceptedTime, actionLatestRecommendationTime,
                        acceptedBy, acceptorType);
        records.add(acceptedActionRecord);
        records.addAll(attachAcceptedActionPoliciesRecord(recommendationId, relatedPolicies));
        return records;
    }

    @Nonnull
    private static Collection<TableRecord<?>> attachAcceptedActionPoliciesRecord(long recommendationId,
            @Nonnull Collection<Long> relatedPolicies) {
        return relatedPolicies.stream()
                .map(policy -> new AcceptedActionsPoliciesRecord(recommendationId, policy))
                .collect(Collectors.toList());
    }

    @Nonnull
    private static List<AcceptedActionInfo> convertToAcceptedActionInfo(
            @Nonnull Result<AcceptedActionsRecord> acceptedActionsRecords,
            @Nonnull Map<Long, Collection<Long>> actionToPoliciesMap) {
        final List<AcceptedActionInfo> acceptedActions =
                new ArrayList<>(acceptedActionsRecords.size());
        for (AcceptedActionsRecord action : acceptedActionsRecords) {
            final Collection<Long> associatedPolicies =
                    actionToPoliciesMap.get(action.getRecommendationId());
            acceptedActions.add(new AcceptedActionInfo(action.getRecommendationId(),
                    action.getLatestRecommendationTime(), action.getAcceptedBy(),
                    action.getAcceptedTime(), action.getAcceptorType(),
                    associatedPolicies != null ? associatedPolicies : Collections.emptyList()));
        }
        return acceptedActions;
    }

    /**
     * Populate policies associated with accepted actions.
     *
     * @param context dsl context
     * @param acceptedRecommendationIds collection of accepted actions recommendationIds
     * @return map contains acceptedRecommendationIds to collection of associated policies.
     */
    @Nonnull
    private static Map<Long, Collection<Long>> populateAssociatedPolicies(
            @Nonnull DSLContext context, @Nonnull List<Long> acceptedRecommendationIds) {
        final Map<Long, Collection<Long>> result = new HashMap<>();
        final Result<AcceptedActionsPoliciesRecord> acceptedActionsPoliciesRecords =
                context.selectFrom(ACCEPTED_ACTIONS_POLICIES)
                        .where(ACCEPTED_ACTIONS_POLICIES.RECOMMENDATION_ID.in(acceptedRecommendationIds))
                        .fetch();
        for (AcceptedActionsPoliciesRecord record : acceptedActionsPoliciesRecords) {
            result.computeIfAbsent(record.getRecommendationId(), (k) -> new ArrayList<>())
                    .add(record.getPolicyId());
        }
        return result;
    }
}
