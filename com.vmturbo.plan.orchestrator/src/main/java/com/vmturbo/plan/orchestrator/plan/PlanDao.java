package com.vmturbo.plan.orchestrator.plan;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.Builder;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;

/**
 * DAO for plan instance.
 */
public interface PlanDao extends DiagsRestorable {

    /**
     * Creates a plan instance, based on plan information.
     *
     * @param planRequest plan information
     * @return plan instance, if created
     * @throws IntegrityException if some integrity constraints violated
     */
    @Nonnull
    PlanInstance createPlanInstance(@Nonnull CreatePlanRequest planRequest) throws IntegrityException;

    /**
     * Creates a plan instance from a scenario object.
     *
     * @param scenario
     * @param planProjectType The type of plan project.
     * @return plan instance, if created
     */
    @Nonnull
    PlanInstance createPlanInstance(@Nonnull final Scenario scenario,
                                    @Nonnull final PlanProjectType planProjectType)
            throws IntegrityException;

    /**
     * Returns all the exising registered plan instances.
     *
     * @return set of existing plan instances.
     */
    @Nonnull
    Set<PlanInstance> getAllPlanInstances();
    /**
     * Returns the plan instance by id, or empty object if there is no plan instance with the
     * specified id.
     *
     * @param id id of the plan to retrieve
     * @return optional value
     */
    @Nonnull
    Optional<PlanInstance> getPlanInstance(long id);

    /**
     * Deletes a plan instance, specified by id.
     *
     * @param id id of the plan instance to delete.
     * @return The plan instance representing the plan before deletion.
     * @throws NoSuchObjectException if plan with the id specified does not exist
     */
    PlanInstance deletePlan(long id) throws NoSuchObjectException;

    /**
     * Update the scenario step of an existing plan. This is used for implementing
     * plan-over-plan.
     *
     * @param planId the ID of the plan to modify
     * @param scenarioId the ID of the scenario to replace for the plan
     * @throws NoSuchObjectException if there is no plan with the given planId
     * @throws IntegrityException if there is no scenario with the given scenarioId
     */
    PlanInstance updatePlanScenario(long planId, long scenarioId) throws NoSuchObjectException,
            IntegrityException;

    /**
     * Updates the existing plan instance. Method creates internally new
     * {@link PlanInstance.Builder}, passes it to {@code updater} and persists.
     *
     * @param planId id of the plan to update
     * @param updater function to perform changes on the plan instance builder
     * @return new version of {@link PlanInstance} after updates
     * @throws IntegrityException if some integrity constraints violated
     * @throws NoSuchObjectException if plan with the id specified does not exist
     * @throws NullPointerException if {@code updater} is null
     */
    PlanInstance updatePlanInstance(long planId, @Nonnull Consumer<Builder> updater)
            throws IntegrityException, NoSuchObjectException;

    /**
     * Add a {@link PlanStatusListener} to the {@link PlanDao}. The listener will receive
     * updates any time any plan's status changes.
     *
     * @param listener The {@link PlanStatusListener}.
     */
    void addStatusListener(@Nonnull final PlanStatusListener listener);

    /**
     *  Return old plans.
     *
     *  @param expirationDate Return all the plans which are older than this date.
     *  @param batchSize Limit the number of returned entries to batchSize.
     *  @return Return the list of expired plans.
     */
    List<PlanInstance> getOldPlans(LocalDateTime expirationDate, int batchSize);

    /**
     * Get the number of plan instances that are currently under execution.
     *
     * @return number of plan instances that are currently under execution
     */
    Integer getNumberOfRunningPlanInstances();

    /**
     * Set the status of the oldest plan instance that is in READY state to QUEUED state,
     * if one exists.
     *
     * @return the plan instance that had just been queued, if any
     */
    Optional<PlanInstance> queueNextPlanInstance();

    /**
     * Set the status of the plan instance to QUEUED if and only if the criteria for queueing
     * an instance are met.
     *
     * @param planInstance Plan instance
     * @return The plan instance that had just been queued, or  {@link Optional#empty()} if the
     *         plan instance does not meet the criteria for queueing.
     * @throws IntegrityException if the plan instance does not pass the integrity check
     */
    Optional<PlanInstance> queuePlanInstance(PlanInstance planInstance)
            throws IntegrityException;
}
