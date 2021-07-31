package com.vmturbo.plan.orchestrator.plan.export;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.PlanExportDTO;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Interface for Plan Destination Dao.
 */
public interface PlanDestinationDao extends DiagsRestorable<Void> {

    /**
     * Returns all the existing plan destinations.
     *
     * @return set of existing plan destinations.
     */
    @Nonnull
    List<PlanExportDTO.PlanDestination> getAllPlanDestinations();

    /**
     * Returns a PlanExportDTO.PlanDestination for the requested externalId.
     *
     * @param externalId for the PlanDestination
     * @return PlanExportDTO.PlanDestination
     */
    @Nonnull
    Optional<PlanExportDTO.PlanDestination> getPlanDestination(String externalId);

    /**
     * Returns a PlanExportDTO.PlanDestination for the requested id.
     *
     * @param id for the PlanDestination
     * @return PlanExportDTO.PlanDestination
     */
    @Nonnull
    Optional<PlanExportDTO.PlanDestination> getPlanDestination(long id);

    /**
     * Create new plan destination record in DB.
     *
     * @param planDestinationDTO info to be inserted to DB
     * @return PlanExportDTO.PlanDestination
     * @throws IntegrityException DB exception
     */
    @Nonnull
    PlanExportDTO.PlanDestination createPlanDestination(PlanExportDTO.PlanDestination planDestinationDTO)
        throws IntegrityException;

    /**
     * update a PlanExportDTO.PlanDestination.
     *
     * @param planDestinationId for the PlanExportDTO.PlanDestination
     * @param newStatus status of PlanDestination
     * @param marketId new market ID, or null to leave unchanged.
     * @return PlanExportDTO.PlanDestination
     * @throws NoSuchObjectException when the original plan destination does not exist.
     * @throws IntegrityException when there is an issue with DB.
     */
    @Nonnull
    PlanExportDTO.PlanDestination updatePlanDestination(long planDestinationId,
                                                        @Nonnull PlanExportStatus newStatus,
                                                        @Nullable Long marketId)
        throws NoSuchObjectException, IntegrityException;

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
    PlanExportDTO.PlanDestination updatePlanDestination(long planDestinationId, PlanExportDTO.PlanDestination planDestination)
        throws NoSuchObjectException, IntegrityException;

    /**
     * Remove PlanDestination record from DB.
     *
     * @param planDestinationId planDestination id
     * @return 0 if failed.
     */
    @Nonnull
    int deletePlanDestination(Long planDestinationId);
}
