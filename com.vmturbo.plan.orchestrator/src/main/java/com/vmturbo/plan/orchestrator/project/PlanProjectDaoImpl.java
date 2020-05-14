package com.vmturbo.plan.orchestrator.project;

import static com.vmturbo.plan.orchestrator.db.tables.PlanProject.PLAN_PROJECT;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import org.apache.commons.collections.CollectionUtils;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.MappingException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject.PlanProjectStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanProject;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanProjectRecord;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * DAO backed by RDBMS to hold plan project.
 */
public class PlanProjectDaoImpl implements PlanProjectDao {

    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final Logger logger = LoggerFactory.getLogger(PlanProjectDaoImpl.class);

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    /**
     * Constructs plan project DAO.
     *
     * @param dsl database access context
     * @param identityInitializer identity generator
     */
    public PlanProjectDaoImpl(@Nonnull final DSLContext dsl,
                              @Nonnull final IdentityInitializer identityInitializer) {
        this.dsl = Objects.requireNonNull(dsl);
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
    }

    @Nonnull
    @Override
    public PlanProjectOuterClass.PlanProject createPlanProject(@Nonnull PlanProjectInfo info)
            throws IntegrityException {
        LocalDateTime curTime = LocalDateTime.now();

        try {
            PlanProject planProject = new
                    PlanProject(IdentityGenerator.next(), curTime, curTime, info,
                    info.getType().name(), PlanProjectStatus.READY.name());
            dsl.newRecord(PLAN_PROJECT, planProject).store();

            return PlanProjectOuterClass.PlanProject.newBuilder()
                    .setPlanProjectId(planProject.getId())
                    .setPlanProjectInfo(info)
                    .build();
        } catch (MappingException dbException) {
            throw new IntegrityException("Unable to create plan project of type "
                    + info.getType().name(), dbException);
        }
    }

    @Nonnull
    @Override
    public Optional<PlanProjectOuterClass.PlanProject> getPlanProject(long id) {
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
    public List<PlanProjectOuterClass.PlanProject> getPlanProjectsByType(@Nonnull final PlanProjectType type) {
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
    public List<PlanProjectOuterClass.PlanProject> getAllPlanProjects() {
        List<PlanProject> planProjects = dsl
                .selectFrom(PLAN_PROJECT)
                .fetch()
                .into(PlanProject.class);

        return planProjects.stream()
                .map(this::toPlanProjectDTO).collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public Optional<PlanProjectOuterClass.PlanProject> deletePlan(long id) {
        return dsl.transactionResult(configuration -> {
            Optional<PlanProjectOuterClass.PlanProject> project =
                    getProject(DSL.using(configuration), id);
            if (project.isPresent()) {
               DSL.using(configuration)
                        .delete(PLAN_PROJECT)
                        .where(PLAN_PROJECT.ID.equal(id))
                        .execute();
            }
            return project;
        });
    }

    /**
     * Look up the DB for the provided plan project id, and convert it to PlanProjectOuterClass.PlanProject, if
     * plan project id exists in DB, otherwise will return Optional.empty().
     *
     * @param context       the DSLContext
     * @param planProjectId the plan project ID
     * @return The optional PlanProjectOuterClass.PlanProject.
     */
    @Nonnull
    private Optional<PlanProjectOuterClass.PlanProject> getProject(@Nonnull final DSLContext context,
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
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public PlanProjectOuterClass.PlanProject updatePlanProject(final long planProjectId,
                                                               @Nullable final PlanProjectStatus newStatus,
                                                               @Nullable final Long mainPlanId,
                                                               @Nullable final List<Long> relatedPlanIds)
            throws IntegrityException, NoSuchObjectException {
        try {
            final PlanProjectOuterClass.PlanProject oldPlanProject = getPlanProject(planProjectId)
                    .orElseThrow(() -> new NoSuchObjectException("Plan project with id "
                            + planProjectId + " not found while trying to update."));
            LocalDateTime now = LocalDateTime.now();
            final PlanProjectInfo.Builder builder = PlanProjectInfo.newBuilder(
                    oldPlanProject.getPlanProjectInfo());
            if (mainPlanId != null) {
                builder.setMainPlanId(mainPlanId);
            }
            if (CollectionUtils.isNotEmpty(relatedPlanIds)) {
                builder.addAllRelatedPlanIds(relatedPlanIds);
            }
            final PlanProjectInfo projectInfo = builder.build();

            PlanProjectStatus projectStatus = newStatus == null
                    ? oldPlanProject.getStatus() : newStatus;
            final PlanProjectOuterClass.PlanProject planProject =
                    PlanProjectOuterClass.PlanProject.newBuilder(oldPlanProject)
                            .setStatus(projectStatus)
                            .setPlanProjectInfo(projectInfo)
                            .build();
            int numRows = dsl.update(PLAN_PROJECT)
                    .set(PLAN_PROJECT.UPDATE_TIME, now)
                    .set(PLAN_PROJECT.STATUS, projectStatus.name())
                    .set(PLAN_PROJECT.PROJECT_INFO, projectInfo)
                    .where(PLAN_PROJECT.ID.eq(planProjectId))
                    .execute();
            if (numRows == 0) {
                throw new NoSuchObjectException("Plan project with id " + planProjectId
                        + " does not exist.");
            }
            return planProject;
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else if (e.getCause() instanceof IntegrityException) {
                throw (IntegrityException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Convert pojos.PlanProject to PlanProjectOuterClass.PlanProject.
     *
     * @param planProject pojos.PlanProject the pojos.PlanProject from JOOQ
     * @return PlanProjectOuterClass.PlanProject the PlanProjectOuterClass.PlanProject from gRPC
     */
    @Nonnull
    private PlanProjectOuterClass.PlanProject toPlanProjectDTO(
            @Nonnull final PlanProject planProject) {
        return PlanProjectOuterClass.PlanProject.newBuilder()
                .setPlanProjectId(planProject.getId())
                .setPlanProjectInfo(planProject.getProjectInfo())
                .build();
    }

    /**
     * {@inheritDoc}
     *
     * This method retrieves all plan projects and serializes them as JSON strings.
     *
     * @return
     * @throws DiagnosticsException
     */
    @Nonnull
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender)
            throws DiagnosticsException {
        final List<PlanProjectOuterClass.PlanProject> planProjects = getAllPlanProjects();
        logger.info("Collecting diagnostics for {} plan projects", planProjects.size());
        for (PlanProjectOuterClass.PlanProject planProject : planProjects) {
            appender.appendString(
                    GSON.toJson(planProject, PlanProjectOuterClass.PlanProject.class));
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method clears all existing plan projects, then deserializes and adds a list of
     * serialized plan projects from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link StringDiagnosable#collectDiags(DiagnosticsAppender)}. Must be in the same order.
     * @throws DiagnosticsException if the db already contains plan projects, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              plan project.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags)
            throws DiagnosticsException {
        final List<String> errors = new ArrayList<>();

        final List<PlanProjectOuterClass.PlanProject> preexisting = getAllPlanProjects();
        if (!preexisting.isEmpty()) {
            final int numPreexisting = preexisting.size();
            final String clearingMessage = "Clearing " + numPreexisting +
                " preexisting plan projects: " + preexisting.stream()
                    .map(planProject -> planProject.getPlanProjectInfo().getName())
                    .collect(Collectors.toList());
            errors.add(clearingMessage);
            logger.warn(clearingMessage);

            final int deleted = deleteAllPlanProjects();
            if (deleted != numPreexisting) {
                final String deletedMessage = "Failed to delete " + (numPreexisting - deleted) +
                    " preexisting plan projects: " + getAllPlanProjects().stream()
                        .map(planProject -> planProject.getPlanProjectInfo().getName())
                        .collect(Collectors.toList());
                logger.error(deletedMessage);
                errors.add(deletedMessage);
            }
        }


        logger.info("Loading {} serialized plan projects from diagnostics", collectedDiags.size());


        final long count = collectedDiags.stream().map(serialized -> {
            try {
                return GSON.fromJson(serialized, PlanProjectOuterClass.PlanProject.class);
            } catch (JsonParseException e) {
                errors.add("Failed to deserialize Plan Project " + serialized +
                    " because of parse exception " + e.getMessage());
                return null;
            }
        }).filter(Objects::nonNull).map(this::restorePlanProject).filter(optional -> {
            optional.ifPresent(errors::add);
            return !optional.isPresent();
        }).count();

        logger.info("Loaded {} plan projects from serialized diagnostics", count);

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return "PlanProjects";
    }

    /**
     * Add a plan project to the database. Note that this is used for restoring plan projects from
     * diags and should NOT be used during normal operations.
     *
     * @param planProject the plan project to add
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restorePlanProject(@Nonnull final PlanProjectOuterClass.PlanProject
                                                        planProject) {
        LocalDateTime curTime = LocalDateTime.now();

        PlanProject record = new PlanProject(planProject.getPlanProjectId(), curTime, curTime,
            planProject.getPlanProjectInfo(), planProject.getPlanProjectInfo().getType().name(),
                planProject.getStatus().name());
        try {
            int r = dsl.newRecord(PLAN_PROJECT, record).store();
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore plan project "
                    + planProject);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore plan project " + planProject +
                " because of DataAccessException "+ e.getMessage());
        }
    }

    /**
     * Deletes all plan projects. Note: this is only used when restoring plan projects
     * from diagnostics and should NOT be used during normal operations.
     *
     * @return the number of records deleted
     */
    private int deleteAllPlanProjects() {
        try {
            return dsl.deleteFrom(PLAN_PROJECT).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }
}
