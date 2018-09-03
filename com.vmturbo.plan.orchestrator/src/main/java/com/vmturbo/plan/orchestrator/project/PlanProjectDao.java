package com.vmturbo.plan.orchestrator.project;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.components.common.diagnostics.Diagnosable;

/**
 * DAO for plan project.
 */
public interface PlanProjectDao extends Diagnosable {

    /**
     * Creates a plan project, based on plan project information.
     *
     * @param info plan information
     * @return plan project, if created
     */
    @Nonnull
    PlanDTO.PlanProject createPlanProject(@Nonnull PlanProjectInfo info);

    /**
     * Returns the plan project by id, or empty object if there is no plan project with the
     * specified id.
     *
     * @param id id of the plan to retrieve
     * @return optional value
     */
    @Nonnull
    Optional<PlanDTO.PlanProject> getPlanProject(long id);

    /**
     * Return a list plan projects selected by project type.
     *
     * @param type Project type
     * @return all plan projects of the specified type
     */
    @Nonnull
    public List<PlanDTO.PlanProject> getPlanProjectsByType(@Nonnull PlanProjectType type);

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
    Optional<PlanDTO.PlanProject> deletePlan(long id);

    SystemLoadInfoResponse getSystemLoadInfo(SystemLoadInfoRequest request);
}
