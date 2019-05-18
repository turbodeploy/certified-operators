package com.vmturbo.api.component.external.api.service;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.plandestination.PlanDestinationApiDTO;
import com.vmturbo.api.serviceinterfaces.IPlanDestinationService;

public class PlanDestinationService implements IPlanDestinationService {

    /**
     * Get Plan destinations.
     *
     * @param businessUnitId, id of the business unit to filter list of plan destinations.
     * @return List of PlanDestinationApiDTO.
     * @throws Exception thrown if issues in converting PlanDestination to PlanDestinationApiDTO.
     */
    @Override
    public @Nonnull List<PlanDestinationApiDTO> getPlanDestinations(@Nullable String businessUnitId)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Get Plan destination by id.
     *
     * @param uuid, The uuid of the plan destination.
     * @return PlanDestinationApiDTO.
     * @throws Exception thrown if issues in converting PlanDestination to PlanDestinationApiDTO.
     */
    @Override
    public PlanDestinationApiDTO getPlanDestinationById(@Nonnull final String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Upload plan results to plan destination.
     *
     * @param planMarketId,       Plan market id, this is the plan we are uploading.
     * @param planDestinationId,  Plan destination id,
     *                            this is the plan destination where we would upload plan results to.
     *
     * @return PlanDestinationApiDTO, with export state and description.
     * @throws Exception thrown if issues in error in upload or invalid plan.
     */
    @Override
    public PlanDestinationApiDTO uploadToPlanDestination(@Nonnull String planMarketId,
                                           @Nonnull String planDestinationId)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
