package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.RejectedActions.REJECTED_ACTIONS;
import static com.vmturbo.action.orchestrator.db.tables.RejectedActionsPolicies.REJECTED_ACTIONS_POLICIES;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.TableRecord;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.tables.records.RejectedActionsPoliciesRecord;
import com.vmturbo.action.orchestrator.db.tables.records.RejectedActionsRecord;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;

/**
 * The {@link RejectedActionsStore} class is used for CRUD operations on rejected actions.
 */
public class RejectedActionsStore implements RejectedActionsDAO {

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
    public RejectedActionsStore(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    @Override
    public void persistRejectedAction(long recommendationId, @Nonnull String rejectedBy,
            @Nonnull LocalDateTime rejectedTime, @Nonnull String rejectingUserType,
            @Nonnull Collection<Long> relatedPolicies) throws ActionStoreOperationException {
        final Collection<TableRecord<?>> inserts = new ArrayList<>(
                createRejectedAction(recommendationId, rejectedBy, rejectedTime, rejectingUserType,
                        relatedPolicies));
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final Result<RejectedActionsRecord> existingRejectionForAction =
                        context.selectFrom(REJECTED_ACTIONS)
                                .where(REJECTED_ACTIONS.RECOMMENDATION_ID.eq(recommendationId))
                                .fetch();
                if (!existingRejectionForAction.isEmpty()) {
                    throw new ActionStoreOperationException(
                            "Action " + recommendationId + " has been already rejected by "
                                    + existingRejectionForAction.stream()
                                    .findFirst()
                                    .get()
                                    .getRejectedBy());
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
    public void removeExpiredRejectedActions(long actionRejectionTTL) {
        dslContext.transaction(configuration -> deleteActionWithExpiredRejection(actionRejectionTTL,
                configuration));
    }

    @Nonnull
    @Override
    public List<RejectedActionInfo> getAllRejectedActions() {
        return dslContext.transactionResult(RejectedActionsStore::selectRejectedActions);
    }

    @Override
    public void removeRejectionsForActionsAssociatedWithPolicy(long policyId) {
        dslContext.transaction(configuration -> deleteRejectedActionsAssociatedWithPolicy(policyId,
                configuration));
    }

    @Nonnull
    private static Collection<TableRecord<?>> createRejectedAction(long recommendationId,
            @Nonnull String rejectedBy, @Nonnull LocalDateTime rejectedTime,
            @Nonnull String rejectingUserType, @Nonnull Collection<Long> relatedPolicies) {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        final RejectedActionsRecord rejectedActionsRecord =
                new RejectedActionsRecord(recommendationId, rejectedTime, rejectedBy,
                        rejectingUserType);
        records.add(rejectedActionsRecord);
        records.addAll(attachRejectedActionPoliciesRecord(recommendationId, relatedPolicies));
        return records;
    }

    @Nonnull
    private static Collection<TableRecord<?>> attachRejectedActionPoliciesRecord(
            long recommendationId, @Nonnull Collection<Long> relatedPolicies) {
        return relatedPolicies.stream()
                .map(policy -> new RejectedActionsPoliciesRecord(recommendationId, policy))
                .collect(Collectors.toList());
    }

    @Nonnull
    private static List<RejectedActionInfo> convertToRejectedActionInfo(
            @Nonnull Result<RejectedActionsRecord> rejectedActionsRecords,
            @Nonnull Map<Long, Collection<Long>> actionToPoliciesMap) {
        final List<RejectedActionInfo> rejectedActions =
                new ArrayList<>(rejectedActionsRecords.size());
        for (RejectedActionsRecord action : rejectedActionsRecords) {
            final Collection<Long> associatedPolicies =
                    actionToPoliciesMap.get(action.getRecommendationId());
            rejectedActions.add(
                    new RejectedActionInfo(action.getRecommendationId(), action.getRejectedBy(),
                            action.getRejectedTime(), action.getRejectorType(),
                            associatedPolicies != null ? associatedPolicies : Collections.emptyList()));
        }
        return rejectedActions;
    }

    private void deleteRejectedActionsAssociatedWithPolicy(long policyId,
            @Nonnull Configuration configuration) {
        final DSLContext context = DSL.using(configuration);
        final Result<RejectedActionsPoliciesRecord> actionsPolicyRecords =
                dslContext.selectFrom(REJECTED_ACTIONS_POLICIES)
                        .where(REJECTED_ACTIONS_POLICIES.POLICY_ID.eq(policyId))
                        .fetch();
        if (!actionsPolicyRecords.isEmpty()) {
            final Set<Long> actionsToDelete = actionsPolicyRecords.stream()
                    .map(RejectedActionsPoliciesRecord::getRecommendationId)
                    .collect(Collectors.toSet());
            context.deleteFrom(REJECTED_ACTIONS)
                    .where(REJECTED_ACTIONS.RECOMMENDATION_ID.in(actionsToDelete))
                    .execute();
            logger.info("Removed rejection for actions `{}` associated with policy {}",
                    actionsToDelete, policyId);
        } else {
            logger.debug("There are no rejected actions associated with policy {} for deleting.",
                    policyId);
        }
    }

    @Nonnull
    private static List<RejectedActionInfo> selectRejectedActions(
            @Nonnull Configuration configuration) {
        final DSLContext context = DSL.using(configuration);
        final Result<RejectedActionsRecord> rejectedActionsRecords =
                context.selectFrom(REJECTED_ACTIONS).fetch();
        final Map<Long, Collection<Long>> actionToPoliciesMap =
                populatePoliciesAssociatedWithRejectedActions(context);
        return convertToRejectedActionInfo(rejectedActionsRecords, actionToPoliciesMap);
    }

    private void deleteActionWithExpiredRejection(long actionRejectionTTLinMinutes,
            @Nonnull Configuration configuration) {
        final DSLContext context = DSL.using(configuration);
        final Result<RejectedActionsRecord> rejectedActions =
                context.selectFrom(REJECTED_ACTIONS).fetch();
        final Map<Long, RejectedActionsRecord> actionsToDelete = rejectedActions.stream()
                .filter(action ->
                        Duration.between(action.getRejectedTime(), LocalDateTime.now()).toMinutes()
                                > actionRejectionTTLinMinutes)
                .collect(Collectors.toMap(RejectedActionsRecord::getRecommendationId,
                        action -> action));
        if (!actionsToDelete.isEmpty()) {
            // information about policies associated with these actions also will be deleted
            // from REJECTED_ACTIONS_POLICIES because this table is setup with delete cascade
            context.deleteFrom(REJECTED_ACTIONS)
                    .where(REJECTED_ACTIONS.RECOMMENDATION_ID.in(actionsToDelete.keySet()))
                    .execute();
            final List<String> deletedActionsInfo = actionsToDelete.values()
                    .stream()
                    .map(el -> "(" + el.getRecommendationId() + ", " + el.getRejectedTime() + ')')
                    .collect(Collectors.toList());
            logger.info("Deleted actions with expired rejection (with information "
                    + "about rejected time): {}", deletedActionsInfo);
        } else {
            logger.debug("There are no actions with expired rejection.");
        }
    }

    /**
     * Populate all policies associated with rejected actions from REJECTED_ACTIONS_POLICIES table.
     *
     * @param context dsl context
     * @return map contains rejectedRecommendationIds to collection of associated policies.
     */
    @Nonnull
    private static Map<Long, Collection<Long>> populatePoliciesAssociatedWithRejectedActions(
            @Nonnull DSLContext context) {
        final Map<Long, Collection<Long>> result = new HashMap<>();
        final Result<RejectedActionsPoliciesRecord> rejectedActionsPoliciesRecords =
                context.selectFrom(REJECTED_ACTIONS_POLICIES).fetch();
        for (RejectedActionsPoliciesRecord record : rejectedActionsPoliciesRecords) {
            result.computeIfAbsent(record.getRecommendationId(), (k) -> new ArrayList<>())
                    .add(record.getPolicyId());
        }
        return result;
    }
}
