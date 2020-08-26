package com.vmturbo.plan.orchestrator.project;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject.PlanProjectStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * DAO for plan project.
 */
public interface PlanProjectDao extends DiagsRestorable<Void> {

    /**
     * Creates a plan project, based on plan project information.
     *
     * @param info plan information
     * @return plan project, if created
     * @throws IntegrityException Thrown when there was an error creating the project.
     */
    @Nonnull
    PlanProjectOuterClass.PlanProject createPlanProject(@Nonnull PlanProjectInfo info)
            throws IntegrityException;

    /**
     * Returns the plan project by id, or empty object if there is no plan project with the
     * specified id.
     *
     * @param id id of the plan to retrieve
     * @return optional value
     */
    @Nonnull
    Optional<PlanProjectOuterClass.PlanProject> getPlanProject(long id);

    /**
     * Return a list plan projects selected by project type.
     *
     * @param type Project type
     * @return all plan projects of the specified type
     */
    @Nonnull
    List<PlanProjectOuterClass.PlanProject> getPlanProjectsByType(@Nonnull PlanProjectType type);

    /**
     * Returns all the existing plan projects.
     *
     * @return set of existing plan projects.
     */
    @Nonnull
    List<PlanProject> getAllPlanProjects();

    /**
     * Deletes a plan project, specified by id.
     *
     * @param id id of the plan project to delete.
     * @return The plan project representing the plan project before deletion.
     */
    @Nonnull
    Optional<PlanProjectOuterClass.PlanProject> deletePlan(long id);

    /**
     * Updated a plan project status, and optionally the info.
     *
     * @param planProjectId plan project id
     * @param newStatus Optional. New status of plan project.
     * @param mainPlanId Optional. Some (migration) projects have a 'main' plan, id of that here.
     * @param relatedPlanIds Optional. Ids of related plans in the project.
     * @return updated plan project
     * @throws IntegrityException Thrown on constraint violation.
     * @throws NoSuchObjectException Thrown when could not fetch record based on id.
     */
    @Nonnull
    PlanProject updatePlanProject(long planProjectId,
                                  @Nullable PlanProjectStatus newStatus,
                                  @Nullable Long mainPlanId,
                                  @Nullable List<Long> relatedPlanIds)
            throws IntegrityException, NoSuchObjectException;

    /**
     * Updates the plan project status to new value.
     *
     * @param planProjectId Id of plan project to update.
     * @param newStatus New plan project status.
     * @return Instance of updated plan project.
     * @throws IntegrityException Thrown on constraint violation.
     * @throws NoSuchObjectException Thrown when could not fetch record based on id.
     */
    @Nonnull
    default PlanProject updatePlanProject(final long planProjectId,
                                          @Nonnull final PlanProjectStatus newStatus)
            throws IntegrityException, NoSuchObjectException {
        return updatePlanProject(planProjectId, newStatus, null, null);
    }
}
