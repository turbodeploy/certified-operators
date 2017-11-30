package com.vmturbo.plan.orchestrator.plan;

import static com.vmturbo.plan.orchestrator.db.tables.PlanInstance.PLAN_INSTANCE;
import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.Builder;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanInstance;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanInstanceRecord;
import com.vmturbo.plan.orchestrator.plan.PlanStatusListener.PlanStatusListenerException;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * DAO backed by RDBMS to hold plan instances.
 */
public class PlanDaoImpl implements PlanDao {

    private final Logger logger = LoggerFactory.getLogger(PlanDaoImpl.class);

    /**
     * Database access context.
     */
    private final DSLContext dsl;
    /**
     * Lock to synchronize operations on {@link #planLocks}.
     */
    private final Object setLock = new Object();
    /**
     * Set, storing all the records' ids, which are marked as locked. Since we are not conforming
     * DB's 1NF, we need to synchronize some java methods in additional to ordinal DB transactional
     * mechanism. I.e. setting plan's status value is purely java-based calculation. We need the
     * synchronization to avoid double notifications ar lack of notifications.
     */
    @GuardedBy("setLock")
    private final Set<Long> planLocks = new HashSet<>();

    private final RepositoryClient repositoryClient;

    private final ActionsServiceBlockingStub actionOrchestratorClient;

    private final StatsHistoryServiceBlockingStub statsClient;

    private final Object listenerLock = new Object();

    @GuardedBy("listenerLock")
    private final List<PlanStatusListener> planStatusListeners = new LinkedList<>();

    /**
     * Constructs plan DAO.
     *
     * @param dsl database access context
     * @param repositoryClient gRPC client for the repository component
     * @param actionOrchestratorClient gRPC client for action orchestrator
     * @param statsClient gRPC client for the stats/history component
     */
    public PlanDaoImpl(@Nonnull final DSLContext dsl,
            @Nonnull final RepositoryClient repositoryClient,
            @Nonnull final ActionsServiceBlockingStub actionOrchestratorClient,
            @Nonnull final StatsHistoryServiceBlockingStub statsClient) {
        this.dsl = Objects.requireNonNull(dsl);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.actionOrchestratorClient = Objects.requireNonNull(actionOrchestratorClient);
        this.statsClient = Objects.requireNonNull(statsClient);
    }

    @Override
    public void addStatusListener(@Nonnull final PlanStatusListener listener) {
        synchronized (listenerLock) {
            planStatusListeners.add(Objects.requireNonNull(listener));
        }
    }

    @Nonnull
    @Override
    public PlanDTO.PlanInstance createPlanInstance(@Nonnull CreatePlanRequest planRequest)
            throws IntegrityException {
        final PlanDTO.PlanInstance.Builder builder = PlanDTO.PlanInstance.newBuilder();
        if (planRequest.hasTopologyId()) {
            builder.setTopologyId(planRequest.getTopologyId());
        }
        if (planRequest.hasScenarioId()) {
            builder.setScenario(ensureScenarioExist(planRequest.getScenarioId()));
        }
        builder.setPlanId(IdentityGenerator.next());
        builder.setStatus(PlanStatus.READY);
        final PlanDTO.PlanInstance plan = builder.build();
        checkPlanConsistency(plan);

        final LocalDateTime curTime = LocalDateTime.now();
        final PlanInstance dbRecord = new PlanInstance(plan.getPlanId(), curTime, curTime, plan);
        dsl.newRecord(PLAN_INSTANCE, dbRecord).store();
        return plan;
    }

    /**
     * Method perform checks on referential integrity of the plan instance.
     *
     * @param plan plan to check
     * @throws IntegrityException whether of any referential checks failed.
     */
    private void checkPlanConsistency(@Nonnull final PlanDTO.PlanInstance plan)
            throws IntegrityException {
        if (plan.hasTopologyId()) {
            ensureTopologyExist(plan.getTopologyId());
        }
    }

    @Nonnull
    @Override
    public Set<PlanDTO.PlanInstance> getAllPlanInstances() {
        final List<PlanInstance> records = dsl.transactionResult(configuration -> {
            final DSLContext context = DSL.using(configuration);
            return dsl.selectFrom(PLAN_INSTANCE).fetch().into(PlanInstance.class);
        });
        return records.stream().map(PlanInstance::getPlanInstance).collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Optional<PlanDTO.PlanInstance> getPlanInstance(long id) {
        return getPlanInstance(dsl, id);
    }

    private static Optional<PlanDTO.PlanInstance> getPlanInstance(@Nonnull final DSLContext dsl,
            final long id) {
        final PlanInstanceRecord planInstance =
                dsl.selectFrom(PLAN_INSTANCE).where(PLAN_INSTANCE.ID.eq(id)).fetchOne();
        if (planInstance == null) {
            return Optional.empty();
        } else {
            return Optional.of(planInstance.into(PlanInstance.class).getPlanInstance());
        }
    }

    @Override
    public PlanDTO.PlanInstance deletePlan(final long id) throws IntegrityException, NoSuchObjectException {
        // For now delete each piece of the plan independently.
        // TODO: implement atomic deletion with rollback. If any piece deletion fails then rollback everything.
        // Delete projected topology from PlanOrchestrator, ActionOrchestrator,
        // Repository and History/Stats
        PlanDTO.PlanInstance plan = getPlanInstance(id).orElseThrow(() -> noSuchObjectException(id));
        deleteRelatedObjects(plan);
        if (dsl.deleteFrom(PLAN_INSTANCE)
                .where(PLAN_INSTANCE.ID.eq(id))
                .execute() != 1) {
            throw noSuchObjectException(id);
        }
        return plan;
    }

    @Override
    public PlanDTO.PlanInstance updatePlanScenario(long planId, long scenarioId) throws NoSuchObjectException,
            IntegrityException {
        Scenario newScenario = ensureScenarioExist(scenarioId);
        return updatePlanInstance(planId, oldPlanInstance ->
                oldPlanInstance.setScenario(newScenario));
    }

    private void deleteRelatedObjects(@Nonnull PlanDTO.PlanInstance plan)
        throws IntegrityException, NoSuchObjectException {

        // TODO - karthikt * Do deletes in parallel
        // TODO - karthikt * Handle failure and retry
        // TODO - karthikt * Delete plan immmediately and delete others in the background
        // TODO - karthikt * For the background delete, we would have to add a delete job(in
        // TODO - karthikt    a queue in a db/local file)
        //
        if (!plan.hasProjectedTopologyId()) {
            logger.info("Skipping projected topology deletion... no topology to delete.");
            return;
        }

        final long projectedTopologyId = plan.getProjectedTopologyId();
        final long topologyContextId = plan.getPlanId();
        logger.info("Deleting projected topology with id:{} and contextId:{} ",
                projectedTopologyId, topologyContextId);

        // Delete topology from Repository
        RepositoryOperationResponse repoResponse =
            repositoryClient.deleteTopology(projectedTopologyId, topologyContextId);

        if (repoResponse.getResponseCode() == RepositoryOperationResponseCode.OK) {
            logger.info("Successfully deleted projected topology with id:{} and"
                    + " contextId:{} from repository",
                    projectedTopologyId, topologyContextId);
        } else {
            throw new IntegrityException("Error trying to delete projected topology with id "
                    + projectedTopologyId + " : "
                    + repoResponse.getError());
        }

        // Delete actions associated with the plan in the ActionsOrchestraor
        try {
            DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .build();
            DeleteActionsResponse actionResponse = actionOrchestratorClient.deleteActions(actionRequest);
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                // If object doesn't exist, just ignore
                logger.info("Actions for planId:{} not found", topologyContextId);
            } else {
                // TODO : karthikt - better error handling. for now just bubble up the exception
                throw sre;
            }
        }

        // Delete plan stats in history component
        DeletePlanStatsRequest statsRequest = DeletePlanStatsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        // If the plan doesn't exist, stats will not throw any exception.
        DeletePlanStatsResponse statsResponse = statsClient.deletePlanStats(statsRequest);
    }

    private static NoSuchObjectException noSuchObjectException(long id) {
        return new NoSuchObjectException("Plan with id " + id + " not found");
    }

    @Override
    public PlanDTO.PlanInstance updatePlanInstance(final long planId,
            @Nonnull final Consumer<Builder> updater)
            throws IntegrityException, NoSuchObjectException {
        Objects.requireNonNull(updater);
        lock(planId);
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final PlanDTO.PlanInstance src = getPlanInstance(context, planId).orElseThrow(
                        () -> new NoSuchObjectException(
                                "Plan with id " + planId + " not found while trying to " +
                                        "update it"));
                final PlanDTO.PlanInstance.Builder newBuilder =
                        PlanDTO.PlanInstance.newBuilder(src);
                updater.accept(newBuilder);
                final PlanDTO.PlanInstance planInstance = newBuilder.build();
                checkPlanConsistency(planInstance);
                final int numRows = context.update(PLAN_INSTANCE)
                        .set(PLAN_INSTANCE.UPDATE_TIME, LocalDateTime.now())
                        .set(PLAN_INSTANCE.PLAN_INSTANCE_, planInstance)
                        .where(PLAN_INSTANCE.ID.eq(planId))
                        .execute();
                if (numRows == 0) {
                    throw new NoSuchObjectException(
                            "Plan with id " + planInstance.getPlanId() + " does not exist");
                }
                if (src.getStatus() != planInstance.getStatus()) {
                    synchronized (listenerLock) {
                        for (final PlanStatusListener listener : planStatusListeners) {
                            try {
                                listener.onPlanStatusChanged(planInstance);
                            } catch (PlanStatusListenerException e) {
                                // TODO Maybe roll back transaction here?
                                logger.error("Error sending plan update notification for plan " +
                                                planId, e);
                            }
                        }
                    }
                }
                return planInstance;
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else if (e.getCause() instanceof IntegrityException) {
                throw (IntegrityException)e.getCause();
            } else {
                throw e;
            }
        } finally {
            unlock(planId);
        }
    }

    private void ensureTopologyExist(final long topologyId) throws IntegrityException {
        // TODO implement
    }

    private Scenario ensureScenarioExist(final long scenarioId) throws IntegrityException {
        final Record1<ScenarioInfo> scenarioRecord = dsl.select(SCENARIO.SCENARIO_INFO)
                .from(SCENARIO)
                .where(SCENARIO.ID.eq(scenarioId))
                .fetchOne();
        if (scenarioRecord == null) {
            throw new IntegrityException("Scenario with id " + scenarioId + " not found");
        } else {
            return Scenario.newBuilder()
                .setId(scenarioId)
                .setScenarioInfo(scenarioRecord.value1())
                .build();
        }
    }

    /**
     * Method locks the specified plan instance from changes. Should be called upon all the
     * methods, capable of changing plan fields.
     *
     * @param planId id of a plan record to lock
     * @see #unlock(long)
     */
    private void lock(final long planId) {
        synchronized (setLock) {
            while (planLocks.contains(planId)) {
                try {
                    setLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread()
                            .interrupt();
                    throw new RuntimeException("Thread interrupted awaiting lock for " + planId, e);
                }
            }
            planLocks.add(planId);
        }
    }

    /**
     * Method unlocks the specified plan instance for changes. Should be called after
     * {@link #lock(long)}.
     *
     * @param planId id of a plan record to unlock
     * @see #lock(long)
     */
    private void unlock(final long planId) {
        synchronized (setLock) {
            planLocks.remove(planId);
            setLock.notifyAll();
        }
    }
}
