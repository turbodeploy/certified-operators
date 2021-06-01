
package com.vmturbo.action.orchestrator.workflow.store;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowFilter;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.identity.store.IdentityStore;

/**
 * In-memory cache of workflows contains all discovered workflows and synchronized with workflows
 * in DB.
 */
public class InMemoryWorkflowStore implements WorkflowStore {

    /**
     * Persistent store for workflows. Used for generating ids of the workflows and persisting
     * discovered workflows in DB.
     * So the only way to obtain the ids of the workflows is through PersistentWorkflowStore.
     */
    private final PersistentWorkflowStore persistentWorkflowStore;

    /**
     * `WorkflowId` -> `Workflow` map.
     */
    private Map<Long, Workflow> workflowIdToWorkflow;

    /**
     * `TargetId` -> `Collection of Workflows` map.
     */
    private Map<Long, Collection<Workflow>> targetIdToWorkflows;

    private static final Logger logger = LogManager.getLogger();

    /**
     * ReadWriteLock used for populating and fetching workflows in in-memory workflow store.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor of {@link InMemoryWorkflowStore}.
     *
     * @param dsl the DSL context for working with DB
     * @param identityStore the identity data store to use
     * @param clock clock to use to track time
     */
    public InMemoryWorkflowStore(@Nonnull DSLContext dsl, @Nonnull IdentityStore identityStore,
            @Nonnull Clock clock) {
        this(new PersistentWorkflowStore(dsl, identityStore, clock));
    }

    /**
     * Visibile for testing so we can replace the persistentWorkflowStore with a mocked one.
     *
     * @param persistentWorkflowStore the persistentWorkflowStore implementation to inject.
     */
    @VisibleForTesting
    /*pkg*/ InMemoryWorkflowStore(@Nonnull PersistentWorkflowStore persistentWorkflowStore) {
        this.persistentWorkflowStore = persistentWorkflowStore;
        try {
            updateInMemoryWorkflowStore();
        } catch (WorkflowStoreException ex) {
            logger.error("InMemory workflow store wasn't initialized due to db issues", ex);
        }
    }

    private void updateInMemoryWorkflowStore() throws WorkflowStoreException {
        final Set<Workflow> workflows =
                persistentWorkflowStore.fetchWorkflows(new WorkflowFilter(Collections.emptyList()));
        lock.writeLock().lock();
        try {
            workflowIdToWorkflow =
                    workflows.stream().collect(Collectors.toMap(Workflow::getId, wf -> wf));
            targetIdToWorkflows = populateTargetIdToWorkflowsMap(workflows);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Map<Long, Collection<Workflow>> populateTargetIdToWorkflowsMap(
            @Nonnull Set<Workflow> workflows) {
        final Map<Long, Collection<Workflow>> result = new HashMap<>();
        workflows.forEach(wf -> result.computeIfAbsent(wf.getWorkflowInfo().getTargetId(),
                el -> new ArrayList<>(workflows.size())).add(wf));
        return result;
    }

    @Nonnull
    @Override
    public Optional<Workflow> fetchWorkflow(long workflowId) throws WorkflowStoreException {
        if (isWorkflowCacheInitialized()) {
            return getWorkflowById(workflowId);
        } else {
            return persistentWorkflowStore.fetchWorkflow(workflowId);
        }
    }

    @Nonnull
    @Override
    public Set<Workflow> fetchWorkflows(@Nonnull WorkflowFilter workflowFilter)
            throws WorkflowStoreException {
        if (isWorkflowCacheInitialized()) {
            return getWorkflowsDiscoveredByTarget(workflowFilter.getDesiredTargetIds());
        } else {
            return persistentWorkflowStore.fetchWorkflows(workflowFilter);
        }
    }

    @Override
    public void persistWorkflows(long targetId, List<WorkflowInfo> workflowInfos)
            throws WorkflowStoreException {
        persistentWorkflowStore.persistWorkflows(targetId, workflowInfos);
        // Synchronise in-memory workflow store after identifying and persisting discovered
        // workflows in DB through PersistentWorkflowStore
        updateInMemoryWorkflowStore();
    }

    @Override
    public long insertWorkflow(final WorkflowInfo workflowInfo) throws WorkflowStoreException {
        long workflowId = persistentWorkflowStore.insertWorkflow(workflowInfo);
        updateInMemoryWorkflowStore();
        return workflowId;
    }

    @Override
    public void updateWorkflow(final long workflowId, final WorkflowInfo workflowInfo) throws WorkflowStoreException {
        persistentWorkflowStore.updateWorkflow(workflowId, workflowInfo);
        updateInMemoryWorkflowStore();
    }

    @Override
    public void deleteWorkflow(final long workflowId) throws WorkflowStoreException {
        persistentWorkflowStore.deleteWorkflow(workflowId);
        updateInMemoryWorkflowStore();
    }

    private Set<Workflow> getWorkflowsDiscoveredByTarget(@Nonnull Collection<Long> targetsIds) {
        lock.readLock().lock();
        try {
            if (targetsIds.isEmpty()) {
                // if certain targets are not specified then we return all existed workflows
                return targetIdToWorkflows.values()
                        .stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
            } else {
                final Set<Workflow> result = new HashSet<>();
                targetsIds.forEach(tId -> {
                    final Collection<Workflow> workflows = targetIdToWorkflows.get(tId);
                    if (workflows != null) {
                        result.addAll(workflows);
                    }
                });
                return result;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private Optional<Workflow> getWorkflowById(long workflowId) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(workflowIdToWorkflow.get(workflowId));
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean isWorkflowCacheInitialized() {
        return workflowIdToWorkflow != null;
    }
}
