package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ActionWorkflowBookKeeping.ACTION_WORKFLOW_BOOK_KEEPING;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.tables.records.ActionWorkflowBookKeepingRecord;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * The {@link AuditActionsStore} class is used for CRUD operations on audited actions.
 */
public class AuditActionsStore implements AuditActionsPersistenceManager {

    /**
     * Database access context.
     */
    private final DSLContext dslContext;

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor of {@link AuditActionsStore}.
     *
     * @param dslContext database access context
     */
    public AuditActionsStore(@Nonnull final DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    @Override
    public void persistActions(@Nonnull Collection<AuditedActionInfo> actionInfos)
            throws ActionStoreOperationException {
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final Collection<ActionWorkflowBookKeepingRecord> auditedActionsRecords = new ArrayList<>();
                actionInfos.forEach(action -> auditedActionsRecords.add(
                        new ActionWorkflowBookKeepingRecord(action.getRecommendationId(),
                                action.getWorkflowId(),
                                !action.getClearedTimestamp().isPresent() ? null
                                        : new Timestamp(action.getClearedTimestamp().get()),
                                action.getTargetEntityId(),
                                action.getSettingName()
                        )));

                // in jooq 3.14.x can be compressed into DSLContext.batchMerge()
                final Query[] queries = auditedActionsRecords.stream()
                        .map(record -> context.insertInto(ACTION_WORKFLOW_BOOK_KEEPING)
                                .set(record)
                                .onDuplicateKeyUpdate()
                                .set(record))
                        .toArray(Query[]::new);
                context.batch(queries).execute();
            });
        } catch (DataAccessException ex) {
            throw new ActionStoreOperationException("Failed to sync up audited actions", ex);
        }
    }

    @Override
    public void removeActionWorkflows(@Nonnull Collection<Pair<Long, Long>> actionsToRemove)
            throws ActionStoreOperationException {
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final Query[] queries = actionsToRemove.stream()
                        .map(record -> context.deleteFrom(ACTION_WORKFLOW_BOOK_KEEPING)
                                .where(ACTION_WORKFLOW_BOOK_KEEPING.ACTION_STABLE_ID.eq(
                                        record.getFirst()))
                                .and(ACTION_WORKFLOW_BOOK_KEEPING.WORKFLOW_ID.eq(record.getSecond())))
                        .toArray(Query[]::new);
                context.batch(queries).execute();
            });
        } catch (DataAccessException ex) {
            throw new ActionStoreOperationException("Failed to remove expired audited actions", ex);
        }
    }

    @Override
    public void removeActionsByRecommendationOid(@Nonnull Collection<Long> recommendationOids)
        throws ActionStoreOperationException {
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                context.deleteFrom(ACTION_WORKFLOW_BOOK_KEEPING)
                    .where(ACTION_WORKFLOW_BOOK_KEEPING.ACTION_STABLE_ID.in(recommendationOids))
                    .execute();
            });
        } catch (DataAccessException ex) {
            throw new ActionStoreOperationException(
                "Failed remove audited actions by recommendation oid", ex);
        }
    }

    @Override
    public Collection<AuditedActionInfo> getActions() {
        final Collection<AuditedActionInfo> auditedActions = new ArrayList<>();
        dslContext.selectFrom(ACTION_WORKFLOW_BOOK_KEEPING)
                .fetch()
                .forEach(record -> auditedActions.add(
                        new AuditedActionInfo(
                            record.getActionStableId(),
                            record.getWorkflowId(),
                            record.getTargetEntityId(),
                            record.getSettingName(),
                            record.getClearedTimestamp() == null ? Optional.empty()
                                        : Optional.of(record.getClearedTimestamp().getTime())
                        )));
        return auditedActions;
    }

    @Override
    public void deleteActionsRelatedToWorkflow(long workflowId) {
        dslContext.transaction(configuration -> {
            final DSLContext context = DSL.using(configuration);
            context.deleteFrom(ACTION_WORKFLOW_BOOK_KEEPING)
                    .where(ACTION_WORKFLOW_BOOK_KEEPING.WORKFLOW_ID.eq(workflowId))
                    .execute();
        });
    }
}
