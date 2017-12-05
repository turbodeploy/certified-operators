package com.vmturbo.plan.orchestrator.plan;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.Builder;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;

/**
 * DAO for plan instance.
 */
public interface PlanDao {

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
}
