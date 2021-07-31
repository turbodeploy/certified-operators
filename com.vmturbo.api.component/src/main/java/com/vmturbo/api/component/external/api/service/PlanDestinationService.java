package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.PlanDestinationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.plandestination.PlanDestinationApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IPlanDestinationService;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationResponse;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationsRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationID;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportResponse;
import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc.PlanExportServiceBlockingStub;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;

public class PlanDestinationService implements IPlanDestinationService {

    private final PlanExportServiceBlockingStub planExportBlockingRpcService;
    private final UuidMapper uuidMapper;

    private PlanDestinationMapper mapper;


    public PlanDestinationService(@Nonnull final PlanExportServiceBlockingStub planExportBlockingRpcService,
                                  @Nonnull final UuidMapper uuidMapper,
                                  @Nonnull final PlanDestinationMapper mapper) {
        this.planExportBlockingRpcService = planExportBlockingRpcService;
        this.uuidMapper = uuidMapper;
        this.mapper = mapper;
    }

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
        List<PlanDestination> planDestinationList = new ArrayList();
        // If there is no business unit identified, return all plan destinations.
        if (businessUnitId == null || businessUnitId.isEmpty()) {
            planExportBlockingRpcService.getPlanDestinations(
                    GetPlanDestinationsRequest.newBuilder().build())
                    .forEachRemaining(p -> planDestinationList.add(p));
        } else {
             if (PlanDestinationMapper.validateUuid(businessUnitId)) {
                planExportBlockingRpcService.getPlanDestinations(GetPlanDestinationsRequest
                        .newBuilder().setAccountId(Long.parseLong(businessUnitId)).build())
                        .forEachRemaining(p -> planDestinationList.add(p));
            }

        }
        List<PlanDestinationApiDTO> planDestinationApiDTOS = new ArrayList();
        for (PlanDestination planDestination : planDestinationList) {
            PlanDestinationApiDTO dto = new PlanDestinationApiDTO();
            mapper.populatePlanDestinationApiDTO(dto, planDestination);
            planDestinationApiDTOS.add(dto);
        }

        return planDestinationApiDTOS;
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
        PlanDestinationMapper.validateUuid(uuid);
        long oid = Long.parseLong(uuid);
        PlanDestinationApiDTO dto = new PlanDestinationApiDTO();
        GetPlanDestinationResponse response = planExportBlockingRpcService.getPlanDestination(
                PlanDestinationID.newBuilder().setId(oid).build());
        if (response.hasDestination()) {
            mapper.populatePlanDestinationApiDTO(dto, response.getDestination());
        } else {
            throw new UnknownObjectException("The given plan destination ID does not match with any"
                    + " object in system.");
        }
        return dto;
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
        final ApiId marketApiId = uuidMapper.fromUuid(planMarketId);
        if (!marketApiId.isPlan()) {
            throw new UnknownObjectException("The ID " + planMarketId + " doesn't belong to a plan.");
        }
        PlanDestinationMapper.validateUuid(planDestinationId);
        long destinationOid = Long.parseLong(planDestinationId);
        PlanDestinationApiDTO dto = new PlanDestinationApiDTO();
        PlanExportResponse response = planExportBlockingRpcService.startPlanExport(
            PlanExportRequest.newBuilder()
                .setMarketId(marketApiId.oid()).setDestinationId(destinationOid).build());

        if (response.hasDestination()) {
            mapper.populatePlanDestinationApiDTO(dto, response.getDestination());
            return dto;
        } else {
            throw new UnknownObjectException("The ID " + planDestinationId
                + " doesn't belong to a plan destination.");
        }
    }
}
