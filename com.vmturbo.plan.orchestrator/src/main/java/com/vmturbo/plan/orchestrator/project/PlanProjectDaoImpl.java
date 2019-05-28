package com.vmturbo.plan.orchestrator.project;

import static com.vmturbo.plan.orchestrator.db.tables.PlanProject.PLAN_PROJECT;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanProject;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanProjectRecord;

/**
 * DAO backed by RDBMS to hold plan project.
 */
public class PlanProjectDaoImpl implements PlanProjectDao {

    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final Logger logger = LoggerFactory.getLogger(PlanProjectDaoImpl.class);

    private final StatsHistoryServiceBlockingStub historyClient;


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
                              @Nonnull final IdentityInitializer identityInitializer,
                              final StatsHistoryServiceBlockingStub historyClient) {
        this.dsl = Objects.requireNonNull(dsl);
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
        this.historyClient = historyClient;
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
    public List<PlanDTO.PlanProject> getPlanProjectsByType(@Nonnull final PlanProjectType type) {
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
        return dsl.transactionResult(configuration -> {
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
    public List<String> collectDiags() throws DiagnosticsException {
        final List<PlanDTO.PlanProject> planProjects = getAllPlanProjects();
        logger.info("Collecting diagnostics for {} plan projects", planProjects.size());
        return planProjects.stream()
            .map(planProject -> GSON.toJson(planProject, PlanDTO.PlanProject.class))
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * This method clears all existing plan projects, then deserializes and adds a list of
     * serialized plan projects from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link Diagnosable#collectDiags()}. Must be in the same order.
     * @throws DiagnosticsException if the db already contains plan projects, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              plan project.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {

        final List<String> errors = new ArrayList<>();

        final List<PlanDTO.PlanProject> preexisting = getAllPlanProjects();
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
                return GSON.fromJson(serialized, PlanDTO.PlanProject.class);
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

    /**
     * Add a plan project to the database. Note that this is used for restoring plan projects from
     * diags and should NOT be used during normal operations.
     *
     * @param planProject the plan project to add
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restorePlanProject(@Nonnull final PlanDTO.PlanProject planProject) {
        LocalDateTime curTime = LocalDateTime.now();

        PlanProject record = new PlanProject(planProject.getPlanProjectId(), curTime, curTime,
            planProject.getPlanProjectInfo(), planProject.getPlanProjectInfo().getType().name());
        try {
            int r = dsl.newRecord(PLAN_PROJECT, record).store();
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore plan project " + planProject);
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

    @Override
    public SystemLoadInfoResponse getSystemLoadInfo(SystemLoadInfoRequest request) {
        SystemLoadInfoResponse systemLoadInfo = historyClient.getSystemLoadInfo(request);
        return systemLoadInfo;
    }
}
