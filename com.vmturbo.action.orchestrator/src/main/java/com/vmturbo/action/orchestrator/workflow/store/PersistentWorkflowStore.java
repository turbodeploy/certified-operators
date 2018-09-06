package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.Workflow.WORKFLOW;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_TARGET_ID;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.action.orchestrator.db.tables.pojos.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.OrchestratorType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStoreUpdate;

/**
 * Persistent Store for Workflow items using SQL DB.
 **/
public class PersistentWorkflowStore implements WorkflowStore {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Jooq persistence handle
     */
    private final DSLContext dsl;

    /**
     * Storage for the OIDs for this
     */
    private final IdentityStore identityStore;

    // injected source for LocalDateTime
    private final Clock clock;

    public PersistentWorkflowStore(@Nonnull DSLContext dsl,
                                   @Nonnull IdentityStore identityStore,
                                   @Nonnull Clock clock) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityStore = Objects.requireNonNull(identityStore);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void persistWorkflows(long targetId, List<WorkflowInfo> workflowInfos)
            throws WorkflowStoreException {
        try {
            // first, grab a set of previously persisted workflow oids for this target
            final Set<Long> previousOidsForThisTarget;
            try {
                previousOidsForThisTarget = identityStore
                        .filterItemOids(getOidMatchingPredicate(targetId));
            } catch (IdentityStoreException e) {
                throw new WorkflowStoreException("Error fetching workflow oids", e);
            }

            // wrap the entire process in a DB transaction
            dsl.transaction(configuration -> {
                // set up a DSLContext for this transaction to use in all Jooq operations
                DSLContext transactionDsl = DSL.using(configuration);
                // first fetch the "old oids" for this target
                // fetch the OIDs for each workflowInfo; as a side effect, new OIDs will be created and
                // persisted if necessary.
                final Map<WorkflowInfo, Long> workflowOids = Maps.newHashMap();
                try {
                    final IdentityStoreUpdate identityStoreUpdate =
                            identityStore.fetchOrAssignItemOids(workflowInfos);
                    workflowOids.putAll(identityStoreUpdate.getNewItems());
                    workflowOids.putAll(identityStoreUpdate.getOldItems());

                    logger.info("{} previous workflow items, {} new workflow items",
                            identityStoreUpdate.getOldItems().size(),
                            identityStoreUpdate.getNewItems().size());
                } catch (IdentityStoreException | DataAccessException e) {
                    logger.error("Identity Store Error fetching ItemOIDs for: " + workflowInfos, e);
                    throw new WorkflowStoreException("Identity Store Error fetching ItemOIDs for: " +
                            workflowInfos, e);
                }

                // capture the time now - use to set last_update_time and then to remove old records
                final LocalDateTime dateTimeNow = LocalDateTime.now(clock);

                // For each workflow, store the info as a blob; the name and targetId
                for (Map.Entry<WorkflowInfo, Long> entry : workflowOids.entrySet()) {
                    WorkflowInfo workflowInfo = entry.getKey();
                    long oid = entry.getValue();
                    try {
                        //todo: batch these writes into a single 'execute()'
                        transactionDsl
                                .insertInto(WORKFLOW)
                                .set(WORKFLOW.ID, oid)
                                .set(WORKFLOW.WORKFLOW_INFO, workflowInfo.toByteArray())
                                .set(WORKFLOW.LAST_UPDATE_TIME, dateTimeNow)
                                .onDuplicateKeyUpdate()
                                .set(WORKFLOW.WORKFLOW_INFO, workflowInfo.toByteArray())
                                .set(WORKFLOW.LAST_UPDATE_TIME, dateTimeNow)
                                .execute();
                    } catch (DataAccessException e) {
                        throw new WorkflowStoreException(String.format("Error persisting workflow:"
                                + " %s for target id %s", workflowInfo.getName(), targetId), e);
                    }
                }

                // now remove all the OIDs that were present before but not in the batch being persisted
                Set<Long> oidsToRemove = Sets.difference(previousOidsForThisTarget,
                        Sets.newHashSet(workflowOids.values()));
                // if any left, they are old; remove them
                if (!oidsToRemove.isEmpty()) {
                    logger.info("Previous workflows removed: {}", oidsToRemove.size());
                    identityStore.removeItemOids(Sets.newHashSet(oidsToRemove));
                }
            });
        } catch (DataAccessException e) {
            throw new WorkflowStoreException(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Set<WorkflowDTO.Workflow> fetchWorkflows(@Nullable OrchestratorType orchestratorTypeFilter)
            throws WorkflowStoreException {
        // note that the 'orchestratorFilterType' is defined by the UI but not set; it is ignored here
        // in the future the filter may need to be implemented, but not currently.
        ImmutableSet.Builder<WorkflowDTO.Workflow> resultBuilder = ImmutableSet.builder();
        try {
            dsl.transaction(configuration -> {
                // set up a DSLContext for this transaction to use in all Jooq operations
                DSLContext transactionDsl = DSL.using(configuration);
                for (com.vmturbo.action.orchestrator.db.tables.pojos.Workflow workflow :
                        transactionDsl.select()
                        .from(WORKFLOW)
                        .fetchInto(Workflow.class)) {
                    // todo: replace the call to the builder with a fetch using a Jooq Converter
                    WorkflowDTO.Workflow workflowInfo = buildWorkflowInfo(workflow);
                    resultBuilder.add(workflowInfo);
                }
            });
            return resultBuilder.build();
        } catch (DataAccessException  e) {
            throw new WorkflowStoreException("Error fetching workflows", e);
        }
    }

    /**
     * Return a Predicate which, given an {@link IdentityMatchingAttributes}, will compare
     * the WORKFLOW_TARGET_ID with the given targetId. Return true if this IdentityMatchinAttributes
     * was persisted for the same targetId.
     *
     * @param targetId the targetId to compare against the WORKFLOW_TARGET_ID
     * @return the predicate to
     */
    private Predicate<IdentityMatchingAttributes> getOidMatchingPredicate(long targetId) {
        String targetIdString = Long.toString(targetId);
        return (IdentityMatchingAttributes foo) -> {
            try {
                return foo.getMatchingAttribute(WORKFLOW_TARGET_ID).getAttributeValue()
                        .equals(targetIdString);
            } catch (IdentityStoreException e) {
                return false;
            }
        };
    }

    /**
     * Build a {@link WorkflowDTO.Workflow} object from a database {@link Workflow} bean.
     * The WorkflowDTO.Workflow protobuf is taken from the 'workflow_info' column, where it was
     * persisted as a blob.
     *
     * @param dbWorkflow the database {@link Workflow} bean to convert
     * @return a {@link WorkflowDTO.Workflow} protobuf containing the information from the DB bean
     * @throws InvalidProtocolBufferException if there's an error converting the blob bytearray
     * back to a protobuf
     */
    private WorkflowDTO.Workflow buildWorkflowInfo(Workflow dbWorkflow)
            throws InvalidProtocolBufferException {
        return WorkflowDTO.Workflow.newBuilder()
                .setId(dbWorkflow.getId())
                .setWorkflowInfo(WorkflowInfo.parseFrom(dbWorkflow.getWorkflowInfo()))
                .build();
    }
}
