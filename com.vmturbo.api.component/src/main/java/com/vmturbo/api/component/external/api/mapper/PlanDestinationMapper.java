package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationNotification;
import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationStatusNotification;
import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationStatusNotification.Status;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.plandestination.PlanDestinationApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;

/**
 *  Converts {@link PlanDestination} objects to the {@link PlanDestinationApiDTO}.
 */
public class PlanDestinationMapper {
    private static final Logger logger = LogManager.getLogger();

    private BusinessAccountRetriever businessAccountRetriever;
    private MarketMapper marketMapper;
    private PlanServiceBlockingStub planBlockingRpcService;

    /**
     * Constructor.
     *
     * @param businessAccountRetriever a retriever for business acocunts.
     * @param marketMapper a mapper to convert {@link MarketApiDTO}.
     * @param planBlockingRpcService the plan rpc service stub.
     */
    public PlanDestinationMapper(@Nonnull final BusinessAccountRetriever businessAccountRetriever,
            @Nonnull final MarketMapper marketMapper,
            @Nonnull final PlanServiceBlockingStub planBlockingRpcService) {
        this.businessAccountRetriever = businessAccountRetriever;
        this.marketMapper = marketMapper;
        this.planBlockingRpcService = planBlockingRpcService;
    }

    /**
     * The utility method to check if the given string is a number.
     *
     * @param uuid uuid string.
     * @return true if it is a number.
     * @throws InvalidOperationException if the uuid string is not a number.
     */
    public static boolean validateUuid(String uuid) throws InvalidOperationException {
        if (!StringUtils.isNumeric(uuid)) {
            throw new InvalidOperationException("Plan destination ID must be numeric. Got: " + uuid);
        }
        return true;
    }

    /**
     * Populate {@link PlanDestinationApiDTO} fields from {@link PlanDestination}.
     *
     * @param dto the {@link PlanDestinationApiDTO}.
     * @param planDestination the {@link PlanDestination}.
     * @throws UnknownObjectException if the business account cannot be found or is invalid.
     * @throws InvalidOperationException If the plan destination has no oid or no business account id.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws StatusRuntimeException if grpc calls run into problem.
     */
    public void populatePlanDestinationApiDTO(PlanDestinationApiDTO dto, PlanDestination planDestination)
            throws UnknownObjectException, InvalidOperationException, ConversionException,
            InterruptedException, StatusRuntimeException {
        if (!planDestination.hasOid()) {
            throw new InvalidOperationException("Plan destination ID must present. ");
        }
        dto.setUuid(String.valueOf(planDestination.getOid()));
        if (planDestination.hasDisplayName()) {
            dto.setDisplayName(planDestination.getDisplayName());
        }
        if (planDestination.hasCriteria()
                && planDestination.getCriteria().hasAccountId()) {
            String uuid = String.valueOf(planDestination.getCriteria().getAccountId());
            dto.setBusinessUnit(businessAccountRetriever.getBusinessAccount(uuid));
        }

        dto.setClassName(planDestination.getClass().getSimpleName());
        if (planDestination.hasStatus()) {
            dto.setExportProgressPercentage(planDestination.getStatus().getProgress());
            dto.setExportState(Status.valueOf(planDestination.getStatus().getState().name()));
            dto.setExportDescription(planDestination.getStatus().getDescription());
        }
        if (planDestination.hasHasExportedData()) {
            dto.setHasExportedData(planDestination.getHasExportedData());
        }
        if (planDestination.hasMarketId()) {
            try {
                dto.setMarket(populateMarketApiDTO(planDestination.getMarketId()));
            } catch (UnknownObjectException ex) {
                logger.warn("Destination {} ({}) associated with nonexistent market {}",
                    planDestination.getDisplayName(), planDestination.getOid(),
                    planDestination.getMarketId(), ex);
            }
        }
    }

    /**
     * Populate {@link MarketApiDTO} fields from plan instance associated with the given market id.
     *
     * @param marketId a given market oid.
     * @return a {@link MarketApiDTO}.
     * @throws UnknownObjectException if the plan instance cannot be found or is invalid.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     * @throws StatusRuntimeException if grpc calls run into problem.
     */
    public MarketApiDTO populateMarketApiDTO(long marketId) throws UnknownObjectException,
            ConversionException, InterruptedException, StatusRuntimeException {
        OptionalPlanInstance planResponse = planBlockingRpcService.getPlan(PlanId.newBuilder()
                .setPlanId(marketId).build());
        if (!planResponse.hasPlanInstance()) {
            throw new UnknownObjectException("Cannot find Plan with OID: " + marketId);
        }
        PlanInstance plan = planResponse.getPlanInstance();
        return marketMapper.dtoFromPlanInstance(plan);
    }

    /**
     * Create a {@link PlanDestinationNotification} for a plan export state transition
     * or progress update.
     *
     * @param destination The updated destination notification.
     * @param isProgressUpdate true if a progress update, false if state change.
     * @return The {@link PlanDestinationNotification} to send to the UI.
     */
    @Nullable
    public PlanDestinationNotification notificationFromPlanDestination(
        @Nonnull final PlanDestination destination, boolean isProgressUpdate) {

        PlanExportStatus newStatus = destination.getStatus();

        String accountName = "";
        long accountId = 0;

        if (destination.hasCriteria() && destination.getCriteria().hasAccountId()) {
            accountId = destination.getCriteria().getAccountId();
            try {
                Collection<BusinessUnitApiDTO> businessAccounts =
                    businessAccountRetriever.getBusinessAccounts(
                        Collections.singleton(accountId));

                if (!businessAccounts.isEmpty()) {
                    accountName = businessAccounts.stream().findAny()
                        .map(BusinessUnitApiDTO::getDisplayName).orElse(accountName);
                }
            } catch (ConversionException | InterruptedException ex) {
                accountName = "(Unknown)";
                logger.warn("Unable to fetch business unit {}", accountId, ex);
            }
        }

        PlanDestinationNotification.Builder builder = PlanDestinationNotification.newBuilder()
            .setPlanDestinationId(String.valueOf(destination.getOid()))
            .setPlanDestinationName(destination.getDisplayName())
            .setPlanDestinationAccountId(String.valueOf(accountId))
            .setPlanDestinationHasExportedData(destination.getHasExportedData())
            .setPlanDestinationAccountName(accountName)
            .setPlanDestinationMarketId(String.valueOf(destination.getMarketId()));

        final PlanDestinationStatusNotification status = PlanDestinationStatusNotification.newBuilder()
            .setStatus(statusFromState(newStatus.getState()))
            .setProgressPercentage(newStatus.getProgress())
            .setDescription(newStatus.getDescription())
            .build();

        if (isProgressUpdate) {
            builder.setPlanDestinationProgressNotification(status);
        } else {
            builder.setPlanDestinationStatusNotification(status);
        }

        return builder.build();
    }

    private static Status statusFromState(@Nonnull PlanExportState state) {
        switch (state) {
            case NONE:
                return Status.NONE;

            case REJECTED:
                return Status.REJECTED;

            case IN_PROGRESS:
                return Status.IN_PROGRESS;

            case SUCCEEDED:
                return Status.SUCCEEDED;

            case FAILED:
                return Status.FAILED;

            default:
                throw new IllegalArgumentException("Unexpected plan destination state: " + state);
        }
    }
}
