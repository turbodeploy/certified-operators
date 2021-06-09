package com.vmturbo.plan.orchestrator.plan;

import static com.vmturbo.plan.orchestrator.db.tables.PlanInstance.PLAN_INSTANCE;
import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import io.grpc.Channel;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.UserContextUtils;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.Builder;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanInstance;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanInstanceRecord;
import com.vmturbo.plan.orchestrator.plan.PlanStatusListener.PlanStatusListenerException;

/**
 * DAO backed by RDBMS to hold plan instances.
 */
public class PlanDaoImpl implements PlanDao {

    /**
     * If a plan is in these states we do not apply timeout logic to it. This is because these states
     * indicate a plan that is not in-progress.
     */
    private static final Set<String> STATES_NOT_ELIGIBLE_FOR_TIMEOUT = ImmutableSet.of(
        PlanStatus.READY.name(), PlanStatus.QUEUED.name(), PlanStatus.FAILED.name(),
        PlanStatus.SUCCEEDED.name(), PlanStatus.STOPPED.name());


    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final Logger logger = LogManager.getLogger();

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

    private final Object listenerLock = new Object();

    private final SettingServiceBlockingStub settingService;

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final Clock clock;

    @GuardedBy("listenerLock")
    private final List<PlanStatusListener> planStatusListeners = new LinkedList<>();

    private final ScheduledExecutorService cleanupExecutor;

    private final OldPlanCleanup oldPlanCleanup;

    PlanDaoImpl(@Nonnull final DSLContext dsl,
                @Nonnull final Channel groupChannel,
                @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
                @Nonnull final SupplyChainServiceBlockingStub supplyChainServiceServiceBlockingStub,
                @Nonnull final Clock clock,
                @Nonnull final ScheduledExecutorService cleanupExecutor,
                final long planTimeout,
                @Nonnull final TimeUnit planTimeoutUnit,
                final long cleanupInterval,
                @Nonnull final TimeUnit cleanupIntervalUnit,
                @Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub) {
        this.dsl = Objects.requireNonNull(dsl);
        this.settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.clock = clock;
        this.cleanupExecutor = cleanupExecutor;
        this.oldPlanCleanup = new OldPlanCleanup(clock, this, planTimeout, planTimeoutUnit);
        this.cleanupExecutor.scheduleAtFixedRate(this.oldPlanCleanup, cleanupInterval, cleanupInterval, cleanupIntervalUnit);
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
        // If specific scenario instance is already specified, use the other method.
        if (planRequest.hasScenarioId() && planRequest.hasProjectType()
                && planRequest.hasPlanProjectId()) {
            return createPlanInstance(planRequest.getScenarioId(), planRequest.getProjectType(),
                    planRequest.getPlanProjectId(), planRequest.getName());
        }

        final PlanDTO.PlanInstance.Builder builder = PlanDTO.PlanInstance.newBuilder();

        // set the created by user, if one is found. Note that unlike the other createPlanInstance
        // method, we aren't defaulting to SYSTEM here if a user is not found. This is because the
        // system-created plans will go through the other createPlanInstance(...) method. If this
        // method does become used for system plans, then we may need to add the SYSTEM user clause
        // here too.
        Optional<String> userId = UserContextUtils.getCurrentUserId();
        if (userId.isPresent()) {
            logger.debug("Setting plan creator to user id {}", userId.get());
            builder.setCreatedByUser(userId.get());
        }

        if (planRequest.hasTopologyId()) {
            builder.setSourceTopologyId(planRequest.getTopologyId());
        }
        if (planRequest.hasScenarioId()) {
            Scenario scenario = ensureScenarioExist(planRequest.getScenarioId());
            builder.setScenario(scenario);
            builder.setName(scenario.getScenarioInfo().getName());
        }
        if (planRequest.hasName()) {
            builder.setName(planRequest.getName());
        }
        builder.setPlanId(IdentityGenerator.next());
        builder.setStatus(PlanStatus.READY);
        builder.setProjectType(PlanProjectType.USER);
        final PlanDTO.PlanInstance plan = builder.build();
        checkPlanConsistency(plan);

        final LocalDateTime curTime = LocalDateTime.now(clock);
        final PlanInstance dbRecord = new PlanInstance(plan.getPlanId(), curTime, curTime, plan,
                plan.getProjectType().name(), PlanStatus.READY.name());
        dsl.newRecord(PLAN_INSTANCE, dbRecord).store();
        return plan;
    }

    /**
     * Create a plan instance in DB with the given scenario (should exist) and project id.
     *
     * @param scenarioId Id of plan existing scenario.
     * @param planProjectType Project type of plan.
     * @param planProjectId Id of plan project.
     * @param planName Name of plan.
     * @return Instance of plan.
     * @throws IntegrityException Thrown on create constraint violation.
     */
    @Nonnull
    private PlanDTO.PlanInstance createPlanInstance(@Nonnull final Long scenarioId,
                                                   @Nonnull final PlanProjectType planProjectType,
                                                   @Nullable final Long planProjectId,
                                                   @Nullable final String planName)
            throws IntegrityException {
        final Scenario scenario = ensureScenarioExist(scenarioId);
        return createPlanInstance(scenario, planProjectType, planProjectId, planName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public PlanDTO.PlanInstance createPlanInstance(@Nonnull final Scenario scenario,
                                                   @Nonnull final PlanProjectType planProjectType,
                                                   @Nullable final Long planProjectId,
                                                   @Nullable final String planName)
            throws IntegrityException {
        final PlanDTO.PlanInstance.Builder planInstanceBuilder = PlanDTO.PlanInstance.newBuilder()
                .setScenario(scenario)
                .setPlanId(IdentityGenerator.next())
                .setStatus(PlanStatus.READY)
                .setProjectType(planProjectType);
        if (planProjectId != null) {
            planInstanceBuilder.setPlanProjectId(planProjectId);
        }
        if (planName != null) {
            planInstanceBuilder.setName(planName);
        }
        // we'll set the createdByUser to either the user from the calling context, or SYSTEM if
        // a user is not found.
        Optional<String> userId = UserContextUtils.getCurrentUserId();
        planInstanceBuilder.setCreatedByUser(userId.orElse(AuditLogUtils.SYSTEM));
        logger.debug("Setting plan creator to user id {}", planInstanceBuilder.getCreatedByUser());

        final PlanDTO.PlanInstance planInstance = planInstanceBuilder.build();
        checkPlanConsistency(planInstance);

        final LocalDateTime curTime = LocalDateTime.now(clock);
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
        if (plan.hasSourceTopologyId()) {
            ensureTopologyExist(plan.getSourceTopologyId());
        }
    }

    @Nonnull
    @Override
    public Set<PlanDTO.PlanInstance> getAllPlanInstances() {
        return getPlans(dsl).stream()
            .map(PlanInstance::getPlanInstance)
            .collect(Collectors.toSet());
    }

    @Nonnull
    @Override
    public Optional<PlanDTO.PlanInstance> getPlanInstance(long id) {
        return getPlanInstance(dsl, id);
    }

    private Optional<PlanDTO.PlanInstance> getPlanInstance(@Nonnull final DSLContext dsl,
            final long id) {
        return getPlans(dsl, PLAN_INSTANCE.ID.eq(id)).stream().findFirst()
            .map(PlanInstance::getPlanInstance);
    }

    @Override
    public PlanDTO.PlanInstance deletePlan(final long id) throws NoSuchObjectException {
        PlanDTO.PlanInstance plan = getPlanInstance(id).orElseThrow(() -> noSuchObjectException(id));
        if (!PlanUtils.canCurrentUserAccessPlan(plan)) {
            // throw an access error if the current user should not be able to delete the plan.
            throw new UserAccessException("User does not have access to plan.");
        }

        if (dsl.deleteFrom(PLAN_INSTANCE)
                .where(PLAN_INSTANCE.ID.eq(id))
                .execute() != 1) {
            throw noSuchObjectException(id);
        }

        planStatusListeners.forEach(listener -> {
            try {
                listener.onPlanDeleted(plan);
            } catch (PlanStatusListenerException e) {
                logger.error("Failed to forward exception to listener: " + listener.getClass().getSimpleName(), e);
            }
        });

        return plan;
    }

    @Override
    public PlanDTO.PlanInstance updatePlanScenario(long planId, long scenarioId) throws NoSuchObjectException,
            IntegrityException {
        Scenario newScenario = ensureScenarioExist(scenarioId);
        return updatePlanInstance(planId, oldPlanInstance ->
                oldPlanInstance.setScenario(newScenario));
    }

    private static NoSuchObjectException noSuchObjectException(long id) {
        return new NoSuchObjectException("Plan with id " + id + " not found");
    }

    @Nonnull
    private List<PlanInstance> getPlans(@Nullable DSLContext context,
                                        @Nonnull Condition... condition) {
        DSLContext targetCtxt = context;
        if (context == null) {
            targetCtxt = dsl;
        }
        return targetCtxt.selectFrom(PLAN_INSTANCE).where(condition)
            .fetch()
            .into(PlanInstance.class);
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
                    () -> new NoSuchObjectException("Plan with id " + planId
                        + " not found while trying to update it"));
                final PlanDTO.PlanInstance.Builder newBuilder =
                        PlanDTO.PlanInstance.newBuilder(src);
                // Note - it is the responsibility of the caller, not the DAO, to ensure that we
                // are not updating a plan when we shouldn't be (e.g. ignore updates received to
                // a plan that was stopped).
                updater.accept(newBuilder);
                final PlanDTO.PlanInstance planInstance = newBuilder.build();
                // If the updater didn't have any effect we don't need to make a database call.
                if (src.equals(planInstance)) {
                    logger.info("Plan {} unchanged after update. Omitting database call.",
                        planInstance.getPlanId());
                    return new PlanUpdateResult(src, src);
                }

                // Don't info print annoying 'from WAITING_FOR_RESULT to WAITING_FOR_RESULT'.
                // Log info only if status code is different or if status messages are different.
                boolean newStatusMessage = !src.getStatusMessage().equals(
                        planInstance.getStatusMessage());
                final Level logLevel = ((src.getStatus() != planInstance.getStatus())
                        || newStatusMessage) ? Level.INFO : Level.DEBUG;
                logger.log(logLevel, "Updating planInstance : {} from {} to {}. {}",
                    planId, src.getStatus().name(), planInstance.getStatus().name(),
                    newStatusMessage ? "New status message: " + planInstance.getStatusMessage()
                        : "");
                checkPlanConsistency(planInstance);
                final int numRows = context.update(PLAN_INSTANCE)
                        .set(PLAN_INSTANCE.UPDATE_TIME, LocalDateTime.now(clock))
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
                        logger.error("Error sending plan update notification for plan "
                            + planId, e);
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
                        PlanStatus.STOPPED.name(),
                        PlanStatus.FAILED.name()))
                .and(PLAN_INSTANCE.STATUS.isNotNull())
                .and(PLAN_INSTANCE.TYPE.notEqual(PlanProjectType.USER.name()))
                .fetchOne()
                .into(Integer.class);
    }

    /**
     * Check if the maximum number of concurrent plan instances has been exceeded.
     *
     * @return true if max number of concurrent plan instances has not reached, false otherwise.
     */
    private boolean isPlanExecutionCapacityAvailable() {
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

        oldPlanCleanup.run();

        // get number of running plan instances
        Integer numRunningInstances = getNumberOfRunningPlanInstances();
        if (numRunningInstances >= maxNumOfRunningInstances) {
            logger.info("No plan execution capacity available, there are {} executing plans "
                    + "which exceeds the maximum execution capacity: {}.",
                numRunningInstances, maxNumOfRunningInstances);
        }
        return numRunningInstances < maxNumOfRunningInstances;
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
            if (isPlanExecutionCapacityAvailable()) {
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
                if (isUserPlan(planInstance)
                        || isPlanExecutionCapacityAvailable()) {
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
                throw (IntegrityException)e.getCause();
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
                .setStartTime(clock.millis())
                .build();
        checkPlanConsistency(updatedInst);
        // do not update planStatus if the status is already STOPPED
        if (originalInst.getStatus() != PlanStatus.STOPPED) {
            context.update(PLAN_INSTANCE)
                    .set(PLAN_INSTANCE.UPDATE_TIME, LocalDateTime.now(clock))
                    .set(PLAN_INSTANCE.PLAN_INSTANCE_, updatedInst)
                    .set(PLAN_INSTANCE.STATUS, updatedInst.getStatus().name())
                    .where(PLAN_INSTANCE.ID.eq(planId))
                    .execute();

            synchronized (listenerLock) {
                for (final PlanStatusListener listener : planStatusListeners) {
                    try {
                        listener.onPlanStatusChanged(updatedInst);
                    } catch (PlanStatusListenerException e) {
                        logger.error("Error sending plan update notification for plan "
                            + planId, e);
                    }
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
    private boolean isUserPlan(@Nonnull final PlanDTO.PlanInstance planInstance) {
        return planInstance.getProjectType().equals(PlanProjectType.USER);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method retrieves all plan instances and serializes them as JSON strings.
     *
     * @throws DiagnosticsException If there is an error collecting diagnostics.
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {

        final Set<PlanDTO.PlanInstance> planInstances = getAllPlanInstances();
        logger.info("Collecting diags for {} plan instances", planInstances.size());

        for (PlanDTO.PlanInstance planInstance: planInstances) {
            appender.appendString(GSON.toJson(planInstance, PlanDTO.PlanInstance.class));
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method clears all existing plan instances, then deserializes and adds a list of
     * serialized plan instances from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      Diagnosable.collectDiags(). Must be in the same order.
     * @throws DiagnosticsException if the db already contains plan instances, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              plan instance.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {

        final List<String> errors = new ArrayList<>();

        final Set<PlanDTO.PlanInstance> preexisting = getAllPlanInstances();
        if (!preexisting.isEmpty()) {
            final int numPreexisting = preexisting.size();
            final String clearingMessage = "Clearing " + numPreexisting
                + " preexisting plan instances: " + preexisting.stream()
                    .map(PlanDTO.PlanInstance::getPlanId)
                    .collect(Collectors.toList());
            errors.add(clearingMessage);
            logger.warn(clearingMessage);

            final int deleted = deleteAllPlanInstances();
            if (deleted != numPreexisting) {
                final String deletedMessage = "Failed to delete " + (numPreexisting - deleted)
                    + " preexisting plan instances: " + getAllPlanInstances().stream()
                        .map(PlanDTO.PlanInstance::getPlanId)
                        .collect(Collectors.toList());
                logger.error(deletedMessage);
                errors.add(deletedMessage);
            }
        }

        logger.info("Loading {} plan instances from diags", collectedDiags.size());

        final long count = collectedDiags.stream().map(serialized -> {
            try {
                return GSON.fromJson(serialized, PlanDTO.PlanInstance.class);
            } catch (JsonParseException e) {
                errors.add("Failed to deserialize plan instance " + serialized
                    + " because of parse exception " + e.getMessage());
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

    @Nonnull
    @Override
    public String getFileName() {
        return "PlanInstances";
    }

    /**
     * Convert a PlanDTO.PlanInstance to a jooq PlanInstance and add it to the database.
     *
     * <p>This is used when restoring serialized PlanDTO.PlanInstances from diagnostics and should
     * not be used for normal operations.
     *
     * @param planInstance the PlanDTO.PlanInstance to convert and add.
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restorePlanInstance(@Nonnull final PlanDTO.PlanInstance planInstance) {
        final LocalDateTime curTime = LocalDateTime.now(clock);
        final PlanInstance record = new PlanInstance(planInstance.getPlanId(), curTime, curTime,
            planInstance, planInstance.getProjectType().name(), planInstance.getStatus().name());
        try {
            final int r = dsl.newRecord(PLAN_INSTANCE, record).store();
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore plan instance " + planInstance);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore plan instance " + planInstance
                + " because of DataAccessException " + e.getMessage());
        }
    }

    /**
     * Deletes all plan instances. Note: this is only used when restoring plan instances
     * from diagnostics and should NOT be used during normal operations.
     *
     * @return the number of records deleted
     */
    private int deleteAllPlanInstances() {
        try {
            return dsl.deleteFrom(PLAN_INSTANCE).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }

    /**
     * Cleans up plan instances if instances are running for more than timeoutHours hours.
     * Since plan instances status could be not be updated, if some components were down
     * during their execution.
     */
    static class OldPlanCleanup implements Runnable {

        private static final Logger logger = LogManager.getLogger();

        private final Clock clock;

        private final PlanDaoImpl planDao;

        private final long planTimeoutSec;

        OldPlanCleanup(@Nonnull final Clock clock,
                       @Nonnull final PlanDaoImpl planDao,
                       final long planTimeout,
                       @Nonnull final TimeUnit planTimeoutUnit) {
            this.clock = clock;
            this.planTimeoutSec = planTimeoutUnit.toSeconds(planTimeout);
            this.planDao = planDao;
        }

        @VisibleForTesting
        long getPlanTimeoutSec() {
            return planTimeoutSec;
        }

        @Override
        public void run() {
            try {
                final LocalDateTime now = LocalDateTime.now(clock);
                final LocalDateTime threshold = now.minusSeconds(planTimeoutSec);
                final List<PlanInstance> expiredInstances = planDao.getPlans(null,
                    PLAN_INSTANCE.STATUS.notIn(STATES_NOT_ELIGIBLE_FOR_TIMEOUT)
                        .and(PLAN_INSTANCE.UPDATE_TIME.lt(threshold)));

                for (PlanInstance expiredInstance : expiredInstances) {
                    try {
                        logger.info("Plan {} has no updates since {},"
                            + " exceeding the timeout threshold of {}. Marking it as failed.",
                            expiredInstance.getId(), expiredInstance.getUpdateTime(),
                            Duration.ofSeconds(planTimeoutSec));
                        planDao.updatePlanInstance(expiredInstance.getId(), (bldr) -> {
                            // It's possible that another operation between the expired instance
                            // query and this update already set the plan to FAILED.
                            if (bldr.getStatus() != PlanStatus.FAILED) {
                                bldr.setStatus(PlanStatus.FAILED);
                                final Duration timeSinceUpdate = Duration.between(expiredInstance.getUpdateTime(), now);
                                bldr.setStatusMessage("Failed due to timeout. No updates for " + timeSinceUpdate.toString());
                            }
                        });
                    } catch (IntegrityException e) {
                        // This shouldn't happen, because we're not changing anything that would violate
                        // integrity. Is the plan already somehow corrupted?
                        logger.warn("Failed to delete expired plan {} because it is no longer valid. Error: {}",
                            expiredInstance.getId(), e.getMessage());
                    } catch (NoSuchObjectException e) {
                        // This may happen if the plan gets deleted on another thread while cleaning up.
                        logger.warn("Failed to delete expired plan {} because it no longer exists.", expiredInstance.getId());
                    }
                }
            } catch (DataAccessException e) {
                logger.error("Failed to clean up expired instances due to SQL exception.", e);
            } catch (RuntimeException e) {
                // We catch runtime exceptions because we don't want a single failed expiration loop
                // to stop subsequent executions of this method in the scheduled executor.
                logger.error("Failed to clean up expired instances due to unexpected exception.", e);
            }
        }
    }
}
