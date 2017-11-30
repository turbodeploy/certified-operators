package com.vmturbo.plan.orchestrator.project;

import static com.vmturbo.plan.orchestrator.db.tables.PlanProject.PLAN_PROJECT;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanProject;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanProjectRecord;

/**
 * DAO backed by RDBMS to hold plan project.
 */
public class PlanProjectDaoImpl implements PlanProjectDao {

    private final Logger logger = LoggerFactory.getLogger(PlanProjectDaoImpl.class);

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    /**
     * Constructs plan project DAO.
     *
     * @param dsl database access context
     */
    public PlanProjectDaoImpl(@Nonnull final DSLContext dsl,
                              @Nonnull final IdentityInitializer identityInitializer) {
        this.dsl = Objects.requireNonNull(dsl);
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
    }

    @Nonnull
    @Override
    public PlanDTO.PlanProject createPlanProject(@Nonnull PlanProjectInfo info) {
        LocalDateTime curTime = LocalDateTime.now();

        PlanProject planProject = new
                PlanProject(IdentityGenerator.next(), curTime, curTime, info,
                info.getType().name());
        dsl.newRecord(PLAN_PROJECT, planProject).store();

        return PlanDTO.PlanProject.newBuilder()
                .setPlanProjectId(planProject.getId())
                .setPlanProjectInfo(info)
                .build();
    }

    @Nonnull
    @Override
    public Optional<PlanDTO.PlanProject> getPlanProject(long id) {
        Optional<PlanProjectRecord> loadedPlanProject = Optional.ofNullable(
                dsl.selectFrom(PLAN_PROJECT)
                        .where(PLAN_PROJECT.ID.eq(id))
                        .fetchAny());
        return loadedPlanProject
                .map(record -> toPlanProjectDTO(record.into(
                        PlanProject.class)));
    }

    @Nonnull
    @Override
    public List<PlanDTO.PlanProject> getPlanProjectsByType(PlanProjectType type) {
        List<PlanProject> planProjects = dsl
                .selectFrom(PLAN_PROJECT)
                .where(PLAN_PROJECT.TYPE.eq(type.name()))
                .fetch()
                .into(PlanProject.class);

        return planProjects.stream()
                .map(this::toPlanProjectDTO).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public List<PlanDTO.PlanProject> getAllPlanProjects() {
        List<PlanProject> planProjects = dsl
                .selectFrom(PLAN_PROJECT)
                .fetch()
                .into(PlanProject.class);

        return planProjects.stream()
                .map(this::toPlanProjectDTO).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public Optional<PlanDTO.PlanProject> deletePlan(long id) {
        Optional<PlanDTO.PlanProject> planProject = dsl.transactionResult(configuration -> {
            Optional<PlanDTO.PlanProject> project =
                    getProject(DSL.using(configuration), id);
            if (project.isPresent()) {
               DSL.using(configuration)
                        .delete(PLAN_PROJECT)
                        .where(PLAN_PROJECT.ID.equal(id))
                        .execute();
            }
            return project;
        });

        return planProject;
    }

    /**
     * Look up the DB for the provided plan project id, and convert it to PlanDTO.PlanProject, if
     * plan project id exists in DB, otherwise will return Optional.empty().
     *
     * @param context       the DSLContext
     * @param planProjectId the plan project ID
     * @return Optional<PlanDTO.PlanProject> the optional PlanDTO.PlanProject
     */
    @Nonnull
    private Optional<PlanDTO.PlanProject> getProject(@Nonnull final DSLContext context,
                                                           final long planProjectId) {
        Optional<PlanProjectRecord> loadedPlanProject = Optional.ofNullable(
                context.selectFrom(com.vmturbo.plan.orchestrator.db.tables.PlanProject.PLAN_PROJECT)
                        .where(com.vmturbo.plan.orchestrator.db.tables.PlanProject.PLAN_PROJECT.ID
                                .eq(planProjectId))
                        .fetchAny());
        return loadedPlanProject
                .map(record -> toPlanProjectDTO(record.into(PlanProject.class)));
    }

    /**
     * Convert pojos.PlanProject to PlanDTO.PlanProject.
     *
     * @param planProject pojos.PlanProject the pojos.PlanProject from JOOQ
     * @return PlanDTO.PlanProject the PlanDTO.PlanProject from gRPC
     */
    @Nonnull
    private PlanDTO.PlanProject toPlanProjectDTO(
            @Nonnull final PlanProject planProject) {
        return PlanDTO.PlanProject.newBuilder()
                .setPlanProjectId(planProject.getId())
                .setPlanProjectInfo(planProject.getProjectInfo())
                .build();
    }
}
