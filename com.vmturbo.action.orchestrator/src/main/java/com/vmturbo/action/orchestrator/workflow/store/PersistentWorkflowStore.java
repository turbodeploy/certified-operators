package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.Workflow.WORKFLOW;
import static com.vmturbo.action.orchestrator.db.tables.WorkflowOid.WORKFLOW_OID;
import static com.vmturbo.action.orchestrator.workflow.store.WorkflowAttributeExtractor.WORKFLOW_TARGET_ID;
import static org.jooq.impl.DSL.select;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.tables.pojos.Workflow;
import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowFilter;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
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

            // wrap the insert/update process in a DB transaction
            dsl.transaction(configuration -> {
                // set up a DSLContext for this transaction to use in all Jooq operations
                DSLContext transactionDsl = DSL.using(configuration);

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
                                .set(WORKFLOW.WORKFLOW_INFO, workflowInfo)
                                .set(WORKFLOW.LAST_UPDATE_TIME, dateTimeNow)
                                .onDuplicateKeyUpdate()
                                .set(WORKFLOW.WORKFLOW_INFO, workflowInfo)
                                .set(WORKFLOW.LAST_UPDATE_TIME, dateTimeNow)
                                .execute();
                    } catch (DataAccessException e) {
                        throw new WorkflowStoreException(String.format("Error persisting workflow:"
                                + " %s for target id %s", workflowInfo.getName(), targetId), e);
                    }
                }
            });

            // now remove all the OIDs that were present before but not in the batch being persisted
            Set<Long> oidsToRemove = Sets.difference(previousOidsForThisTarget,
                    Sets.newHashSet(workflowOids.values()));
            // if any left, they are old; remove them
            if (!oidsToRemove.isEmpty()) {
                identityStore.removeItemOids(Sets.newHashSet(oidsToRemove));
                logger.info("Previous workflows removed: {}", oidsToRemove.size());
            }
        } catch (DataAccessException | IdentityStoreException e) {
            throw new WorkflowStoreException(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Set<WorkflowDTO.Workflow> fetchWorkflows(@Nonnull WorkflowFilter workflowFilter)
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
                        .where(createConditions(workflowFilter))
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
     * {@inheritDoc}
     *
     * <p>Fetch the desired row from the WORKFLOW table with the given ID. This row might not be found,
     * causing an error, or (if the DB is inconsistent) more than one row might be returned, also
     * an error.
     */
    @Nonnull
    public Optional<WorkflowDTO.Workflow> fetchWorkflow(long workflowId) throws WorkflowStoreException {
        WorkflowDTO.Workflow.Builder workflowBuilder = WorkflowDTO.Workflow.newBuilder()
                .setId(workflowId);
        try {
            dsl.transaction(configuration -> {
                // set up a DSLContext for this transaction to use in all Jooq operations
                DSLContext transactionDsl = DSL.using(configuration);
                WorkflowInfo workflowInfo =
                        transactionDsl.select(WORKFLOW.WORKFLOW_INFO)
                                .from(WORKFLOW)
                                .where(WORKFLOW.ID.eq(workflowId))
                                .fetchOneInto(WorkflowInfo.class);
                // if the workflow by that id is not found, then the select() result will be null
                if (workflowInfo != null) {
                    workflowBuilder.setWorkflowInfo(workflowInfo);
                }
            });
        } catch (DataAccessException  e) {
            throw new WorkflowStoreException("Error fetching workflow: " + workflowId, e);
        }
        // if the workflow info was found, it was stored in the workflowBuilder; else not found
        return workflowBuilder.hasWorkflowInfo()
                ? Optional.of(workflowBuilder.build())
                : Optional.empty();
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

     */
    private WorkflowDTO.Workflow buildWorkflowInfo(Workflow dbWorkflow) {
        return WorkflowDTO.Workflow.newBuilder()
                .setId(dbWorkflow.getId())
                .setWorkflowInfo(dbWorkflow.getWorkflowInfo())
                .build();
    }

    @Nonnull
    private Collection<Condition> createConditions(@Nonnull WorkflowFilter workflowFilter) {
        final ImmutableList.Builder<Condition> condBuilder = ImmutableList.builder();
        final List<Long> desiredTargets = workflowFilter.getDesiredTargetIds();
        if (!desiredTargets.isEmpty()) {
            condBuilder.add(WORKFLOW.ID.in(select(WORKFLOW_OID.ID).from(WORKFLOW_OID)
                    .where(WORKFLOW_OID.TARGET_ID.in(desiredTargets))));
        }
        return condBuilder.build();
    }
}
