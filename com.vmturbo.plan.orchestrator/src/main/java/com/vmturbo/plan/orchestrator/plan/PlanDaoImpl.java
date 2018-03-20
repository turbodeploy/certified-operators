package com.vmturbo.plan.orchestrator.plan;

import static com.vmturbo.plan.orchestrator.db.tables.PlanInstance.PLAN_INSTANCE;
import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;

import java.time.LocalDateTime;
import java.util.ArrayList;
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
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.StringUtils;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.Builder;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanInstance;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanInstanceRecord;
import com.vmturbo.plan.orchestrator.plan.PlanStatusListener.PlanStatusListenerException;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * DAO backed by RDBMS to hold plan instances.
 */
public class PlanDaoImpl implements PlanDao {

    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

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

    private final SettingServiceBlockingStub settingService;

    private final int planTimeOutHours;

    @GuardedBy("listenerLock")
    private final List<PlanStatusListener> planStatusListeners = new LinkedList<>();

    /**
     * Constructs plan DAO.
     *
     * @param dsl database access context
     * @param repositoryClient gRPC client for the repository component
     * @param actionOrchestratorClient gRPC client for action orchestrator
     * @param statsClient gRPC client for the stats/history component
     * @param planTimeOutHours plan time out hours
     */
    public PlanDaoImpl(@Nonnull final DSLContext dsl,
                       @Nonnull final RepositoryClient repositoryClient,
                       @Nonnull final ActionsServiceBlockingStub actionOrchestratorClient,
                       @Nonnull final StatsHistoryServiceBlockingStub statsClient,
                       @Nonnull final Channel groupChannel,
                       final int planTimeOutHours) {
        this.dsl = Objects.requireNonNull(dsl);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.actionOrchestratorClient = Objects.requireNonNull(actionOrchestratorClient);
        this.statsClient = Objects.requireNonNull(statsClient);
        this.settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
        this.planTimeOutHours = planTimeOutHours;
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
        builder.setProjectType(PlanProjectType.USER);
        final PlanDTO.PlanInstance plan = builder.build();
        checkPlanConsistency(plan);

        final LocalDateTime curTime = LocalDateTime.now();
        final PlanInstance dbRecord = new PlanInstance(plan.getPlanId(), curTime, curTime, plan,
                plan.getProjectType().name(), PlanStatus.READY.name());
        dsl.newRecord(PLAN_INSTANCE, dbRecord).store();
        return plan;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public PlanDTO.PlanInstance createPlanInstance(@Nonnull final Scenario scenario,
                                                   @Nonnull final PlanProjectType planProjectType)
            throws IntegrityException {
        final PlanDTO.PlanInstance planInstance = PlanDTO.PlanInstance.newBuilder()
                .setScenario(scenario)
                .setPlanId(IdentityGenerator.next())
                .setStatus(PlanStatus.READY)
                .setProjectType(planProjectType)
                .build();
        checkPlanConsistency(planInstance);

        final LocalDateTime curTime = LocalDateTime.now();
        final PlanInstance dbRecord =
                new PlanInstance(planInstance.getPlanId(), curTime, curTime, planInstance,
                        planProjectType.name(), PlanStatus.READY.name());
        dsl.newRecord(PLAN_INSTANCE, dbRecord).store();
        return planInstance;
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
        return records.stream()
            .map(PlanInstance::getPlanInstance)
            .collect(Collectors.toSet());
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
    public PlanDTO.PlanInstance deletePlan(final long id) throws NoSuchObjectException {
        // For now delete each piece of the plan independently.
        // TODO: implement atomic deletion with rollback. If any piece deletion fails then rollback everything.
        // Delete projected topology from PlanOrchestrator, ActionOrchestrator,
        // Repository and History/Stats
        PlanDTO.PlanInstance plan = getPlanInstance(id).orElseThrow(() -> noSuchObjectException(id));
        // First delete all the plan related data in other components. Then
        // delete the data in plan db. This ordering is to ensure that we don't leave
        // orphan/dangling plan data in the other components. There can still be
        // some dangling plan data as we haven't handled all the error cases.
        // But this atleast minimizes the number of dangling objects.
        // TODO - karthikt - The deleteRelatedObjects function should be moved
        // outside this class where the deletePlan is called as DAO classes should
        // concern itself only with access to the DB.
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

    private void deleteRelatedObjects(@Nonnull PlanDTO.PlanInstance plan) {

        // TODO - karthikt * Do deletes in parallel
        // TODO - karthikt * Handle failure and retry
        // TODO - karthikt * Delete plan immmediately and delete others in the background
        // TODO - karthikt * For the background delete, we would have to add a delete job(in
        // TODO - karthikt    a queue in a db/local file)
        //
        final List<String> errors = new ArrayList<>();
        final long topologyContextId = plan.getPlanId();
        if (plan.hasProjectedTopologyId()) {
            final long projectedTopologyId = plan.getProjectedTopologyId();
            logger.info("Deleting projected topology with id:{} and contextId:{} ",
                    projectedTopologyId, topologyContextId);

            // Delete topology from Repository
            try {
                final RepositoryOperationResponse repoResponse =
                        repositoryClient.deleteTopology(projectedTopologyId, topologyContextId);
                if (repoResponse.getResponseCode() == RepositoryOperationResponseCode.OK) {
                    logger.info("Successfully deleted projected topology with id:{} and"
                                    + " contextId:{} from repository",
                            projectedTopologyId, topologyContextId);
                } else {
                    errors.add("Error trying to delete projected topology with id "
                            + projectedTopologyId + " : "
                            + repoResponse.getError());
                }
            } catch (StatusRuntimeException e) {
                errors.add("Failed to delete projected topology " + projectedTopologyId +
                        " due to error: " + e.getLocalizedMessage());
            }
        } else {
            logger.info("Skipping projected topology deletion for plan {}... no topology to delete.",
                    topologyContextId);
        }

        // Delete actions associated with the plan in the ActionsOrchestraor
        if (plan.hasActionPlanId()) {
            final DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .build();
            try {
                actionOrchestratorClient.deleteActions(actionRequest);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    // If object doesn't exist, just ignore
                    logger.info("Actions for planId:{} not found", topologyContextId);
                } else {
                    errors.add("Failed to delete actions associated with plan " + topologyContextId +
                            " due to error: " + e.getLocalizedMessage());
                }
            }
        } else {
            logger.info("Skipping action plan deletion for plan {}. No action plan to delete.",
                    topologyContextId);
        }

        if (plan.hasStatsAvailable()) {
            // Delete plan stats in history component
            final DeletePlanStatsRequest statsRequest = DeletePlanStatsRequest.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .build();

            try {
                // If the plan doesn't exist, stats will not throw any exception.
                statsClient.deletePlanStats(statsRequest);
            } catch (StatusRuntimeException e) {
                errors.add("Failed to delete stats associated with plan " + topologyContextId +
                        " due to error: " + e.getLocalizedMessage());
            }
        } else {
            logger.info("Skipping stats deletion for plan {}. No stats available to delete.",
                    topologyContextId);
        }

        if (!errors.isEmpty()) {
            logger.error("Encountered errors trying to delete plan {}. Errors:\n", plan.getPlanId(),
                    StringUtils.join(errors, "\n"));
        } else {
            logger.info("Successfully deleted all known related objects for plan {}", topologyContextId);
        }
    }

    private static NoSuchObjectException noSuchObjectException(long id) {
        return new NoSuchObjectException("Plan with id " + id + " not found");
    }

    @Override
    public PlanDTO.PlanInstance updatePlanInstance(final long planId,
            @Nonnull final Consumer<Builder> updater)
            throws IntegrityException, NoSuchObjectException {
        Objects.requireNonNull(updater);
        final PlanUpdateResult updateResult;
        lock(planId);
        try {
            updateResult = dsl.transactionResult(configuration -> {
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
                        .set(PLAN_INSTANCE.STATUS, planInstance.getStatus().name())
                        .where(PLAN_INSTANCE.ID.eq(planId))
                        .execute();
                if (numRows == 0) {
                    throw new NoSuchObjectException(
                            "Plan with id " + planInstance.getPlanId() + " does not exist");
                }
                return new PlanUpdateResult(src, planInstance);
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

        if (updateResult.oldPlan.getStatus() != updateResult.newPlan.getStatus()) {
            synchronized (listenerLock) {
                for (final PlanStatusListener listener : planStatusListeners) {
                    try {
                        listener.onPlanStatusChanged(updateResult.newPlan);
                    } catch (PlanStatusListenerException e) {
                        logger.error("Error sending plan update notification for plan " +
                                planId, e);
                    }
                }
            }
        }

        return updateResult.newPlan;
    }

    @Override
    public List<PlanDTO.PlanInstance> getOldPlans(LocalDateTime expirationDate, int batchSize)
        throws DataAccessException {

        final List<PlanInstance> records =
            dsl.select()
                .from(PLAN_INSTANCE)
                .where(PLAN_INSTANCE.UPDATE_TIME.lt(expirationDate))
                .orderBy(PLAN_INSTANCE.UPDATE_TIME)
                .limit(batchSize)
                .fetch()
                .into(PlanInstance.class);

        return records.stream()
            .map(PlanInstance::getPlanInstance)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getNumberOfRunningPlanInstances() {
        return dsl.selectCount()
                .from(PLAN_INSTANCE)
                .where(PLAN_INSTANCE.STATUS.notIn(
                        PlanStatus.READY.name(),
                        PlanStatus.SUCCEEDED.name(),
                        PlanStatus.FAILED.name()))
                .and(PLAN_INSTANCE.STATUS.isNotNull())
                .and(PLAN_INSTANCE.TYPE.notEqual(PlanProjectType.USER.name()))
                .and(PLAN_INSTANCE.TYPE.notEqual(PlanProjectType.INITAL_PLACEMENT.name()))
                .fetchOne()
                .into(Integer.class);
    }

    /**
     * Check if the maximum number of concurrent plan instances has been exceeded.
     *
     * @return true if max number of concurrent plan instances has not reached, false otherwise.
     */
    private boolean isPlanExecutionCapacityAvailable(DSLContext dslContext, final int timeOutHours) {
        // get maximum number of concurrent plan instance allowed
        GetGlobalSettingResponse response = settingService.getGlobalSetting(
            GetSingleGlobalSettingRequest.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.MaxConcurrentPlanInstances
                        .getSettingName())
                .build());

        final float maxNumOfRunningInstances;
        if (response.hasSetting()) {
            maxNumOfRunningInstances = response.getSetting().getNumericSettingValue().getValue();
        } else {
            maxNumOfRunningInstances =
                GlobalSettingSpecs.MaxConcurrentPlanInstances.createSettingSpec()
                        .getNumericSettingValueType().getDefault();
        }

        LocalDateTime expirationHour = LocalDateTime.now().minusHours(timeOutHours);
        cleanUpFailedInstance(expirationHour);

        // get number of running plan instances
        Integer numRunningInstances = getNumberOfRunningPlanInstances();
        if (numRunningInstances >= maxNumOfRunningInstances) {
            logger.info("No plan execution capacity available, there are {} executing plans " +
                            "which exceeds the maximum execution capacity: {}.",
                    numRunningInstances, maxNumOfRunningInstances);
        }
        return numRunningInstances < maxNumOfRunningInstances;
    }

    /**
     * Clean up plan instances if instances are running for more than timeoutHours hours.
     * Since plan instances status could be not be updated, if some components were down
     * during their execution.
     *
     * @param timeOutHours plan time out hours
     * @throws DataAccessException db access exception
     */
    private void cleanUpFailedInstance(final LocalDateTime timeOutHours) throws DataAccessException {
        dsl.update(PLAN_INSTANCE)
                .set(PLAN_INSTANCE.UPDATE_TIME, LocalDateTime.now())
                .set(PLAN_INSTANCE.STATUS, PlanStatus.FAILED.name())
                .where(PLAN_INSTANCE.UPDATE_TIME.lt(timeOutHours),
                        PLAN_INSTANCE.STATUS.notIn(
                                PlanStatus.READY.name(),
                                PlanStatus.SUCCEEDED.name(),
                                PlanStatus.FAILED.name()))
                .and(PLAN_INSTANCE.STATUS.isNotNull())
                .execute();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<PlanDTO.PlanInstance> queueNextPlanInstance() {
        // Run this logic in a transaction.
        return dsl.transactionResult(configuration -> {
            final DSLContext context = DSL.using(configuration);

            // Proceed only if the maximum number of concurrent plan instances have not been exceeded
            if (isPlanExecutionCapacityAvailable(context, planTimeOutHours)) {
                // Select the instance record that is in READY state and has the oldest creation time
                // Call "forUpdate()" to lock the record for subsequent update.
                final PlanInstanceRecord planInstanceRecord = context.selectFrom(PLAN_INSTANCE)
                        .where(PLAN_INSTANCE.STATUS.eq(PlanStatus.READY.name()))
                        .orderBy(PLAN_INSTANCE.CREATE_TIME.asc())
                        .limit(1)
                        .forUpdate()
                        .fetchOne();
                if (planInstanceRecord == null) {
                    // No plan instance in READY state.
                    return Optional.empty();
                } else {
                    // Update the plan instance with the "QUEUED" status.
                    return Optional.of(setQueuedStatus(context, planInstanceRecord));
                }
            } else {
                return Optional.empty();
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<PlanDTO.PlanInstance> queuePlanInstance(final PlanDTO.PlanInstance planInstance)
            throws IntegrityException {
        try {
            // Run this logic in a transaction.
            return dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);

                // Proceed only if the maximum number of concurrent plan instances have not been exceeded
                if (isUserOrInitialPlacementPlan(planInstance)
                        || isPlanExecutionCapacityAvailable(context, planTimeOutHours)) {
                    // Select the instance record that is in READY state and has the given ID.
                    // Call "forUpdate()" to lock the record for subsequent update.
                    final PlanInstanceRecord planInstanceRecord = context.selectFrom(PLAN_INSTANCE)
                            .where(PLAN_INSTANCE.ID.eq(planInstance.getPlanId()))
                            .forUpdate()
                            .fetchOne();
                    if (planInstanceRecord == null) {
                        // This situation should not happen as the planInstance is passed in.
                        return Optional.empty();
                    } else if (planInstanceRecord.get(PLAN_INSTANCE.STATUS).equals(PlanStatus.READY.name())) {
                        // Change the status of the plan instance to QUEUED, indicating that the
                        // execution of this instance can proceed.
                        return Optional.of(setQueuedStatus(context, planInstanceRecord));
                    } else {
                        // Plan instance exists, but it has already queued or started execution.
                        // Return the instance without changing the status.
                        return Optional.empty();
                    }
                } else {
                    // The plan instance is a system plan and the maximum number of concurrent plans
                    // has reached.  Return the plan instance without changing the status to QUEUED.
                    return Optional.empty();
                }
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof IntegrityException) {
                throw (IntegrityException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Given a plan instance database record, set the status column to the "QUEUED" status.
     * Update the serialized protobuf object with the new status and sets the start time.
     * Also notify plan status listeners of the changed status.
     * Run this method in the transaction where the planInstanceRecord is retrieved.
     *
     * @param context  DSL Context
     * @param planInstanceRecord plan instance database record
     * @return the protobuf object of the updated plan instance
     * @throws IntegrityException if the protobuf object fails consistency check.
     */
    private PlanDTO.PlanInstance setQueuedStatus(DSLContext context, PlanInstanceRecord planInstanceRecord)
            throws IntegrityException {
        // Update the plan instance with the "QUEUED" status.
        long planId = planInstanceRecord.get(PLAN_INSTANCE.ID);
        PlanDTO.PlanInstance originalInst = planInstanceRecord.into(PlanInstance.class)
                .getPlanInstance();
        PlanDTO.PlanInstance updatedInst = PlanDTO.PlanInstance.newBuilder(originalInst)
                .setStatus(PlanStatus.QUEUED)
                .setStartTime(System.currentTimeMillis())
                .build();
        checkPlanConsistency(updatedInst);
        context.update(PLAN_INSTANCE)
                .set(PLAN_INSTANCE.UPDATE_TIME, LocalDateTime.now())
                .set(PLAN_INSTANCE.PLAN_INSTANCE_, updatedInst)
                .set(PLAN_INSTANCE.STATUS, updatedInst.getStatus().name())
                .where(PLAN_INSTANCE.ID.eq(planId))
                .execute();

        synchronized (listenerLock) {
            for (final PlanStatusListener listener : planStatusListeners) {
                try {
                    listener.onPlanStatusChanged(updatedInst);
                } catch (PlanStatusListenerException e) {
                    logger.error("Error sending plan update notification for plan " +
                            planId, e);
                }
            }
        }
        return updatedInst;
    }

    /**
     * The result of updating a plan instance. This is a wrapper class to return
     * two values from a {@link org.jooq.TransactionalCallable}.
     */
    @Immutable
    private static class PlanUpdateResult {
        private final PlanDTO.PlanInstance oldPlan;
        private final PlanDTO.PlanInstance newPlan;

        private PlanUpdateResult(@Nonnull final PlanDTO.PlanInstance oldPlan,
                         @Nonnull final PlanDTO.PlanInstance newPlan) {
            this.oldPlan = oldPlan;
            this.newPlan = newPlan;
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

    /**
     * Check if plan instance is a user created plan or initial placement plan.
     *
     * @param planInstance {@link PlanInstance} needs to check.
     * @return return true if it is a user created plan or initial placement plan.
     */
    private boolean isUserOrInitialPlacementPlan(@Nonnull final PlanDTO.PlanInstance planInstance) {
        return planInstance.getProjectType().equals(PlanProjectType.USER) ||
            planInstance.getProjectType().equals(PlanProjectType.INITAL_PLACEMENT);
    }

    /**
     * {@inheritDoc}
     *
     * This method retrieves all plan instances and serializes them as JSON strings.
     *
     * @return a list of serialized plan instances
     * @throws DiagnosticsException
     */
    @Nonnull
    @Override
    public List<String> collectDiags() throws DiagnosticsException {

        final Set<PlanDTO.PlanInstance> planInstances = getAllPlanInstances();
        logger.info("Collecting diags for {} plan instances", planInstances.size());

        return planInstances.stream()
            .map(planInstance -> GSON.toJson(planInstance, PlanDTO.PlanInstance.class))
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * This method deserializes and adds a list of serialized plan instances from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      Diagnosable.collectDiags(). Must be in the same order.
     * @throws DiagnosticsException if the db already contains plan instances, or in response
     *                              to any errors that may occur unserializing or restoring a
     *                              plan instance.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {

        if (!getAllPlanInstances().isEmpty()) {
            throw new DiagnosticsException("Plan instances cannot be restored because they are already present");
        }
        final List<String> errors = new ArrayList<>();

        logger.info("Loading {} plan instances from diags", collectedDiags.size());

        final long count = collectedDiags.stream().map(serialized -> {
            try {
                return GSON.fromJson(serialized, PlanDTO.PlanInstance.class);
            } catch (JsonParseException e) {
                errors.add("Failed to deserialize plan instance " + serialized +
                    " because of parse exception " + e.getMessage());
                return null;
            }
        }).filter(Objects::nonNull).map(this::restorePlanInstance).filter(optional -> {
            optional.ifPresent(errors::add);
            return !optional.isPresent();
        }).count();

        logger.info("Loaded {} plan instances from diags", count);

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    /**
     * Convert a PlanDTO.PlanInstance to a jooq PlanInstance and add it to the database.
     *
     * This is used when restoring serialized PlanDTO.PlanInstances from diagnostics and should
     * not be used for normal operations.
     *
     * @param planInstance the PlanDTO.PlanInstance to convert and add.
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restorePlanInstance(@Nonnull final PlanDTO.PlanInstance planInstance) {
        final LocalDateTime curTime = LocalDateTime.now();
        final PlanInstance record = new PlanInstance(planInstance.getPlanId(), curTime, curTime,
            planInstance, planInstance.getProjectType().name(), planInstance.getStatus().name());
        try {
            final int r = dsl.newRecord(PLAN_INSTANCE, record).store();
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore plan instance " + planInstance);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore plan instance " + planInstance +
                " because of DataAccessException "+ e.getMessage());
        }
    }
}
