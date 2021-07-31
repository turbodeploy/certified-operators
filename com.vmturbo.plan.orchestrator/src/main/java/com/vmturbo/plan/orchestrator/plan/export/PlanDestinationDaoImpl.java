package com.vmturbo.plan.orchestrator.plan.export;

import static com.vmturbo.plan.orchestrator.db.tables.PlanDestination.PLAN_DESTINATION;

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

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.MappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.plan.PlanExportDTO;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanDestination;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanDestinationRecord;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Implementation for Plan Destination Dao.
 */
public class PlanDestinationDaoImpl implements PlanDestinationDao {
    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final Logger logger = LoggerFactory.getLogger(PlanDestinationDaoImpl.class);

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    /**
     * Constructs plan destination DAO.
     *
     * @param dsl database access context
     * @param identityInitializer identity generator
     */
    public PlanDestinationDaoImpl(@Nonnull final DSLContext dsl,
                              @Nonnull final IdentityInitializer identityInitializer) {
        this.dsl = Objects.requireNonNull(dsl);
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
    }

    @Nonnull
    @Override
    public List<PlanExportDTO.PlanDestination> getAllPlanDestinations() {
        List<PlanDestination> planDestinations = dsl
            .selectFrom(PLAN_DESTINATION)
            .fetch()
            .into(PlanDestination.class);

        return planDestinations.stream()
            .map(this::toPlanDestinationDTO).collect(Collectors.toList());
    }

    /**
     * Returns a PlanExportDTO.PlanDestination for the requested externalId.
     *
     * @param externalId for the PlanDestination
     * @return PlanExportDTO.PlanDestination
     */
    @Nonnull
    public Optional<PlanExportDTO.PlanDestination> getPlanDestination(String externalId) {
        Optional<PlanDestinationRecord> loadedPlanDestination = Optional.ofNullable(
            dsl.selectFrom(PLAN_DESTINATION)
                .where(PLAN_DESTINATION.EXTERNAL_ID.eq(externalId))
                .fetchAny());
        return loadedPlanDestination.map(record -> toPlanDestinationDTO(record.into(PlanDestination.class)));
    }

    /**
     * Returns a PlanExportDTO.PlanDestination for the requested id.
     *
     * @param id for the PlanDestination
     * @return PlanExportDTO.PlanDestination
     */
    @Nonnull
    public Optional<PlanExportDTO.PlanDestination> getPlanDestination(long id) {
        Optional<PlanDestinationRecord> loadedPlanDestination = Optional.ofNullable(
            dsl.selectFrom(PLAN_DESTINATION)
                .where(PLAN_DESTINATION.ID.eq(id))
                .fetchAny());
        return loadedPlanDestination.map(record -> toPlanDestinationDTO(record.into(PlanDestination.class)));
    }

    /**
     * Create new plan destination record in DB.
     *
     * @param planDestinationDTO info to be inserted to DB
     * @return PlanExportDTO.PlanDestination
     * @throws IntegrityException DB exception
     */
    @Nonnull
    public PlanExportDTO.PlanDestination createPlanDestination(PlanExportDTO.PlanDestination planDestinationDTO) throws IntegrityException {
        LocalDateTime curTime = LocalDateTime.now();

        // update DTO with the information which is controlled by PO
        final long id = IdentityGenerator.next();
        final PlanExportDTO.PlanDestination newPlanDestinationDTO = PlanExportDTO.PlanDestination.newBuilder(planDestinationDTO)
            .setOid(id)
            .setStatus(PlanExportStatus.newBuilder().setState(PlanExportState.NONE).setProgress(0))
            .build();

        // insert record to DB
        try {
            PlanDestination planDestination = new PlanDestination(
                id,
                newPlanDestinationDTO.getExternalId(),
                curTime,
                null,
                newPlanDestinationDTO.getStatus().getState().toString(),
                newPlanDestinationDTO.getTargetId(),
                newPlanDestinationDTO);
            dsl.newRecord(PLAN_DESTINATION, planDestination).store();

            return toPlanDestinationDTO(planDestination);
        } catch (MappingException dbException) {
            throw new IntegrityException("Unable to create plan destination with name " + planDestinationDTO.getDisplayName(), dbException);
        }
    }

    /**
     * update a PlanExportDTO.PlanDestination.
     *
     * @param planDestinationId for the PlanExportDTO.PlanDestination
     * @param newStatus status of PlanDestination
     * @param marketId new marketId to be updated, or null to leave unchanged
     * @return PlanExportDTO.PlanDestination
     * @throws NoSuchObjectException when the original plan destination does not exist.
     * @throws IntegrityException when issue with DB.
     */
    @Nonnull
    public PlanExportDTO.PlanDestination updatePlanDestination(final long planDestinationId,
                                                               @Nonnull PlanExportStatus newStatus,
                                                               @Nullable final Long marketId)
        throws NoSuchObjectException, IntegrityException {
        try {
            final PlanExportDTO.PlanDestination oldPlanDestination = getPlanDestination(planDestinationId)
                .orElseThrow(() -> new NoSuchObjectException("Plan destination with id " + planDestinationId + " not found while trying to update."));

            // Create new planDestinationDTO object with the updated information
            PlanExportDTO.PlanDestination.Builder newPlanDestinationDTObuilder =
                PlanExportDTO.PlanDestination.newBuilder(oldPlanDestination)
                .setStatus(newStatus);

            if (marketId != null) {
                newPlanDestinationDTObuilder.setMarketId(marketId);
            }

            LocalDateTime now = LocalDateTime.now();

            int numRows = dsl.update(PLAN_DESTINATION)
                .set(PLAN_DESTINATION.UPDATE_TIME, now)
                .set(PLAN_DESTINATION.STATUS, newStatus.getState().toString())
                .set(PLAN_DESTINATION.PLAN_DESTINATION_, newPlanDestinationDTObuilder.build())
                .where(PLAN_DESTINATION.ID.eq(oldPlanDestination.getOid()))
                .execute();
            if (numRows == 0) {
                throw new NoSuchObjectException("Plan destination with id " + planDestinationId
                    + " does not exist.");
            }

            // return the object stored in DB after update
            final PlanExportDTO.PlanDestination newPlanDestination = getPlanDestination(planDestinationId)
                .orElseThrow(() -> new NoSuchObjectException("Plan destination with id " + planDestinationId + " not found while trying to update."));
            return newPlanDestination;
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
     * update a PlanExportDTO.PlanDestination.  Used by update for discovery.
     *
     * @param planDestinationId for the PlanExportDTO.PlanDestination
     * @param planDestination planDestination
     * @return PlanExportDTO.PlanDestination
     * @throws NoSuchObjectException when the original plan destination does not exist.
     * @throws IntegrityException when issue with DB.
     */
    @Nonnull
    public PlanExportDTO.PlanDestination updatePlanDestination(long planDestinationId, PlanExportDTO.PlanDestination planDestination)
        throws NoSuchObjectException, IntegrityException {
        try {
            final PlanExportDTO.PlanDestination oldPlanDestination = getPlanDestination(planDestinationId)
                .orElseThrow(() -> new NoSuchObjectException("Plan destination with id " + planDestinationId + " not found while trying to update."));

            // preserve the status, oid, last update time, and marketId
            PlanExportDTO.PlanDestination.Builder newPlanDestinationBuilder = PlanExportDTO.PlanDestination.newBuilder(planDestination)
                .setOid(oldPlanDestination.getOid())
                .setStatus(oldPlanDestination.getStatus());
            if (oldPlanDestination.hasMarketId()) {
                newPlanDestinationBuilder.setMarketId(oldPlanDestination.getMarketId());
            } else if (planDestination.hasMarketId()) {
                newPlanDestinationBuilder.clearMarketId();
            }

            LocalDateTime now = LocalDateTime.now();
            int numRows = dsl.update(PLAN_DESTINATION)
                .set(PLAN_DESTINATION.DISCOVERY_TIME, now)
                .set(PLAN_DESTINATION.PLAN_DESTINATION_, newPlanDestinationBuilder.build())
                .where(PLAN_DESTINATION.ID.eq(oldPlanDestination.getOid()))
                .execute();
            if (numRows == 0) {
                throw new NoSuchObjectException("Plan destination with id " + planDestinationId
                    + " does not exist.");
            }

            // return the object stored in DB after update
            final PlanExportDTO.PlanDestination result = getPlanDestination(planDestinationId)
                .orElseThrow(() -> new NoSuchObjectException("Plan destination with id " + planDestinationId + " not found while trying to update."));
            return result;
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
     * Remove PlanDestination record from DB.
     *
     * @param planDestinationId planDestination id
     * @return 0 if failed.
     */
    @Nonnull
    public int deletePlanDestination(Long planDestinationId) {
        try {
            return dsl.deleteFrom(PLAN_DESTINATION).where(PLAN_DESTINATION.ID.eq(planDestinationId)).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }

    /**
     * Remove all PlanDestination records from DB.
     *
     * @return number of records removed
     */
    @Nonnull
    private int deleteAllPlanDestination() {
        try {
            return dsl.deleteFrom(PLAN_DESTINATION).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }

    /**
     * Convert pojos.PlanDestination to PlanExportDTO.PlanDestination.
     *
     * @param planDestination pojos.PlanDestination the pojos.PlanDestination from JOOQ
     * @return PlanExportDTO.PlanDestination the PlanExportDTO.PlanDestination from gRPC
     *
     */
    @VisibleForTesting
    @Nonnull
    PlanExportDTO.PlanDestination toPlanDestinationDTO(@Nonnull final PlanDestination planDestination) {
        PlanExportDTO.PlanDestination planDestinationDTO = PlanExportDTO.PlanDestination.newBuilder(planDestination.getPlanDestination())
            .setOid(planDestination.getId())
            .build();
        return planDestinationDTO;
    }

    /**
     * {@inheritDoc}
     * This method clears all existing plan destinations, then deserializes and adds a list of
     * serialized plan destinations from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link StringDiagnosable#collectDiags(DiagnosticsAppender)}. Must be in the same order.
     * @param context diags restore context.
     * @throws DiagnosticsException if the db already contains plan destinations, or in response
     *                 to any errors that may occur deserializing or restoring a plan destination.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, final Void context) throws DiagnosticsException {
        final List<String> errors = new ArrayList<>();

        final List<PlanExportDTO.PlanDestination> preexisting = getAllPlanDestinations();

        // delete all from DB
        if (!preexisting.isEmpty()) {
            final int numPreexisting = preexisting.size();
            final String clearingMessage = "Clearing " + numPreexisting
                +  " preexisting plan destinations: " + preexisting.stream()
                .map(planDestination -> planDestination.getDisplayName())
                .collect(Collectors.toList());
            errors.add(clearingMessage);
            logger.warn(clearingMessage);

            final int deleted = deleteAllPlanDestination();
            if (deleted != numPreexisting) {
                final String deletedMessage = "Failed to delete " + (numPreexisting - deleted)
                    + " preexisting plan destination: " + getAllPlanDestinations().stream()
                    .map(planDestination -> planDestination.getDisplayName())
                    .collect(Collectors.toList());
                logger.error(deletedMessage);
                errors.add(deletedMessage);
            }
        }

        logger.info("Loading {} serialized plan destinations from diagnostics", collectedDiags.size());

        final long count = collectedDiags.stream().map(serialized -> {
            try {
                return GSON.fromJson(serialized, PlanExportDTO.PlanDestination.class);
            } catch (JsonParseException e) {
                errors.add("Failed to deserialize Plan Destination " + serialized
                    + " because of parse exception " + e.getMessage());
                return null;
            }
        }).filter(Objects::nonNull).map(this::restorePlanDestination).filter(optional -> {
            optional.ifPresent(errors::add);
            return !optional.isPresent();
        }).count();
    }


    private Optional<String> restorePlanDestination(@Nonnull final PlanExportDTO.PlanDestination planDestinationDto) {
        LocalDateTime curTime = LocalDateTime.now();

        PlanDestination record = new PlanDestination(planDestinationDto.getOid(),
            planDestinationDto.getExternalId(), curTime, curTime,
            planDestinationDto.getStatus().getState().toString(),
            planDestinationDto.getTargetId(), planDestinationDto);
        try {
            int r = dsl.newRecord(PLAN_DESTINATION, record).store();
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore plan plan destination "
                + planDestinationDto);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore plan destination " + planDestinationDto
                 + " because of DataAccessException " + e.getMessage());
        }
    }


    @Nonnull
    @Override
    public String getFileName() {
        return "PlanDestinations";
    }

    /**
     * This method retrieves all plan destinations and serializes them as JSON strings.
     *
     * @param appender an appender to put diagnostics to. String by string.
     * @throws DiagnosticsException exception during appending to diagnostics.
     */
    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        final List<PlanExportDTO.PlanDestination> planDestinations = getAllPlanDestinations();
        logger.info("Collecting diagnostics for {} plan destinations", planDestinations.size());
        for (PlanExportDTO.PlanDestination planDestination : planDestinations) {
            appender.appendString(
                GSON.toJson(planDestination, PlanExportDTO.PlanDestination.class));
        }
    }
}
