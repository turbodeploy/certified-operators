package com.vmturbo.plan.orchestrator.project;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;

/**
 * DAO for plan project.
 */
public interface PlanProjectDao extends DiagsRestorable {

    /**
     * Creates a plan project, based on plan project information.
     *
     * @param info plan information
     * @return plan project, if created
     */
    @Nonnull
    PlanProjectOuterClass.PlanProject createPlanProject(@Nonnull PlanProjectInfo info);

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
}
