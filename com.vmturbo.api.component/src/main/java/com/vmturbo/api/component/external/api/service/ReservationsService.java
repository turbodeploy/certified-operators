package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.enums.ReservationAction;
import com.vmturbo.api.enums.ReservationEditAction;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IReservationsService;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationDTO.CreateReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * XL implementation of IReservationAndDeployService.
 **/
public class ReservationsService implements IReservationsService {
    private static final Logger logger = LogManager.getLogger();

    private static final int MINIMUM_PLACEMENT_COUNT = 1;

    public static final Set<String> RESERVATION_RELATED_COMMODITIES
            = Stream.of(
                CommodityType.CPU_PROVISIONED,
                CommodityType.MEM_PROVISIONED,
                CommodityType.CPU,
                CommodityType.MEM,
                CommodityType.IO_THROUGHPUT,
                CommodityType.NET_THROUGHPUT,
                CommodityType.STORAGE_AMOUNT,
                CommodityType.STORAGE_ACCESS,
                CommodityType.STORAGE_PROVISIONED)
          .map(CommodityTypeMapping::getMixedCaseFromCommodityType)
          .collect(Collectors.toSet());

    private final int maximumPlacementCount;

    private final ReservationServiceBlockingStub reservationService;

    private final ReservationMapper reservationMapper;

    private final StatsService statsService;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final UuidMapper uuidMapper;


    ReservationsService(@Nonnull final ReservationServiceBlockingStub reservationService,
                        @Nonnull final ReservationMapper reservationMapper, final int maximumPlacementCount,
                        @Nonnull final StatsService statsService,
                        @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                        @Nonnull final UuidMapper uuidMapper) {
        this.reservationService = Objects.requireNonNull(reservationService);
        this.reservationMapper = Objects.requireNonNull(reservationMapper);
        this.maximumPlacementCount = maximumPlacementCount;
        this.statsService = statsService;
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.uuidMapper = uuidMapper;
    }

    @Override
    public List<DemandReservationApiDTO> getAllReservations(Map<String, String> queryParams)
                    throws Exception {
        String reservationStatus = queryParams.get(ParamStrings.STATUS);
        if (!Strings.isNullOrEmpty(reservationStatus)) {
            return getReservationsByStatus(reservationStatus.toUpperCase());
        } else {
            return getAllReservations();
        }
    }

    @Override
    public List<StatApiDTO> getReservationAwareStats(String entityId)
            throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(entityId);
        Set<Long> providers = new HashSet<>();
        if (apiId.isGroup()) {
            final GetMembersRequest.Builder membersBuilder =
                    GetMembersRequest.newBuilder().setExpectPresent(true).addId(apiId.oid());
            final Iterator<GetMembersResponse> groupMembersResp =
                    groupServiceBlockingStub.getMembers(membersBuilder.build());
            providers.addAll(groupMembersResp.next().getMemberIdList());
        } else {
            providers.add(apiId.oid());
        }

        if (providers.isEmpty()) {
            throw new OperationFailedException(
                    "No provider found for entity: " + entityId);
        }
        List<DemandReservationApiDTO> reservations = getAllReservations();
        List<ResourceApiDTO> resourceApiDTOs = new ArrayList<>();
        for (DemandReservationApiDTO reservationApiDTO : reservations) {
            for (DemandEntityInfoDTO demandEntityInfoDTO : reservationApiDTO.getDemandEntities()) {
                // getComputeResources and getStorageResources will be null for all reservations
                // whose status is not RESERVED.
                if (demandEntityInfoDTO.getPlacements() != null
                        && demandEntityInfoDTO.getPlacements().getComputeResources() != null
                        && demandEntityInfoDTO.getPlacements().getStorageResources() != null
                        && !reservationApiDTO.getReservationDeployed()) {
                    resourceApiDTOs.addAll(demandEntityInfoDTO.getPlacements().getComputeResources());
                    resourceApiDTOs.addAll(demandEntityInfoDTO.getPlacements().getStorageResources());
                }
            }
        }

        StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        statPeriodApiInputDTO.setStatistics(new ArrayList<>());
        // trim the stats to just the resources related to reservation.
        for (String commName : RESERVATION_RELATED_COMMODITIES) {
            StatApiInputDTO statApiInputDTO = new StatApiInputDTO();
            statApiInputDTO.setName(commName);
            statPeriodApiInputDTO.getStatistics().add(statApiInputDTO);
        }
        List<StatSnapshotApiDTO> entityStatSnapshot = statsService.getStatsByEntityQuery(entityId,
                statPeriodApiInputDTO);

        // entityStatSnapshot should be a singleton.
        if (entityStatSnapshot.size() != 1 || entityStatSnapshot.get(0).getStatistics() == null) {
            throw new OperationFailedException(
                    "Failed to collect stats for entity: " + entityId);
        }

        List<StatApiDTO> entityStats = entityStatSnapshot.get(0).getStatistics();
        trimReservationRelatedStats(entityStats, providers.size());
        // add all the reservation related used values to the entityStats.
        for (ResourceApiDTO resourceApiDTO : resourceApiDTOs) {
            // if the reservation has a provider for which the stats is asked for
            // add the utilisation of reservation to the used value of the provider.
            if (providers.stream().anyMatch(providerId -> resourceApiDTO
                    .getProvider().getUuid().equals(providerId.toString()))) {
                // we are taking the entity stats and comparing them to the resource stats.
                // If the stats name matches then add the stats together.
                entityStats.stream().forEach(
                        entityStat -> {
                            Optional<StatApiDTO> vmStatOptional =
                                    resourceApiDTO.getStats().stream()
                                            .filter(stat -> stat.getName()
                                                    .equals(entityStat.getName())).findFirst();
                            if (vmStatOptional.isPresent()) {
                                StatApiDTO vmStat = vmStatOptional.get();
                                StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
                                statValueApiDTO.setAvg(entityStat.getValues().getAvg()
                                        + vmStat.getValues().getAvg());
                                entityStat.setValues(statValueApiDTO);
                            }
                        }
                );
            }
        }
        return entityStats;

    }

    /**
     * Remove all other entries other average attribute of capacity and values.
     * Convert the stats from average to aggregate by multiplying with entitySize.
     *
     * @param stats the stats which need to be trimmed.
     * @param entitySize the number of members in the entity.
     */
    private void trimReservationRelatedStats(List<StatApiDTO> stats, float entitySize) {
        stats.removeIf(stat -> stat.getCapacity() == null || stat.getValues() == null);
        List<StatApiDTO> trimmedStats = new ArrayList<>();
        stats.stream().forEach(stat -> {
            // create a new StatApiDTO with just name , units, values and capacity.
            StatApiDTO trimmedStat = new StatApiDTO();
            trimmedStat.setUnits(stat.getUnits());
            trimmedStat.setName(stat.getName());
            StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
            statValueApiDTO.setAvg(stat.getValues().getAvg() * entitySize);
            trimmedStat.setValues(statValueApiDTO);
            StatValueApiDTO statCapacityApiDTO = new StatValueApiDTO();
            statCapacityApiDTO.setAvg(stat.getCapacity().getAvg() * entitySize);
            trimmedStat.setCapacity(statCapacityApiDTO);
            trimmedStats.add(trimmedStat);
        });
        stats.clear();
        stats.addAll(trimmedStats);
    }

    private List<DemandReservationApiDTO> getAllReservations() throws Exception {
        GetAllReservationsRequest request = GetAllReservationsRequest.newBuilder()
                .build();
        Iterable<Reservation> reservationIterable = () -> reservationService.getAllReservations(request);
        final List<DemandReservationApiDTO> result = new ArrayList<>();
        for (Reservation reservation : reservationIterable) {
            result.add(reservationMapper.convertReservationToApiDTO(reservation));
        }
        return result;
    }

    @Override
    public DemandReservationApiDTO getReservationByID(@Nonnull String reservationID,
                                                      @Nonnull  Boolean apiCallBlock) throws Exception {
        try {
            final GetReservationByIdRequest request = GetReservationByIdRequest.newBuilder()
                    .setReservationId(Long.valueOf(reservationID))
                    .setApiCallBlock(apiCallBlock)
                    .build();
            final Reservation reservation =
                    reservationService.getReservationById(request);
            return reservationMapper.convertReservationToApiDTO(reservation);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    @Override
    public DemandReservationApiDTO createReservationForDemand(
            @Nonnull Boolean apiCallBlock,
            @Nonnull ReservationAction demandAction,
            @Nonnull DemandReservationApiInputDTO demandApiInputDTO) throws Exception {
        // We do not support deployment in XL
        switch (demandAction) {
            case PLACEMENT:
                return new DemandReservationApiDTO();
            case RESERVATION:
                logger.info("Received Reservation: " + demandApiInputDTO.getDemandName());
                validatePlacementCount(demandApiInputDTO);
                final Reservation reservation = reservationMapper.convertToReservation(demandApiInputDTO);
                final CreateReservationRequest request = CreateReservationRequest.newBuilder()
                        .setReservation(reservation)
                        .build();
                final Reservation createdReservation = reservationService.createReservation(request);
                return reservationMapper.convertReservationToApiDTO(createdReservation);
            default:
                throw new UnsupportedOperationException("Invalid action " + demandAction);
        }
    }

    /**
     * Restrict the minimum and maximum placement count. Minimum count is 1 by default.
     *
     * @param inputDTO input DTO for reservation request.
     * @throws OperationFailedException when the placement count is larger than max count or less than min count.
     */
    private void validatePlacementCount(@Nonnull final DemandReservationApiInputDTO inputDTO)
            throws OperationFailedException {
        if (inputDTO.getParameters() != null && inputDTO.getParameters()
                .stream()
                        .anyMatch(dto -> placementCountOutOfRange(dto))) {
            throw new OperationFailedException(
                   "The minimum placement count is " + MINIMUM_PLACEMENT_COUNT
                         + ". The maximum placement count is " + maximumPlacementCount
                         + ". Please change the placement count and try again.");
        }
    }

    private boolean placementCountOutOfRange(DemandReservationParametersDTO dto) {
        final int count = dto.getPlacementParameters().getCount();
        return count < MINIMUM_PLACEMENT_COUNT || count > maximumPlacementCount;
    }

    @Override
    public DemandReservationApiDTO doActionOnReservationByID(Boolean callBlock,
                                                             ReservationEditAction action, String reservationID) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Boolean deleteReservationByID(String reservationID, Boolean deployed) {
        final DeleteReservationByIdRequest request = DeleteReservationByIdRequest.newBuilder()
                .setReservationId(Long.valueOf(reservationID))
                .setDeployed(deployed)
                .build();
        reservationService.deleteReservationById(request);
        return true;
    }

    @Override
    public DemandReservationApiDTO deployReservationByID(Boolean callBlock, String reservationID)
                    throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    private List<DemandReservationApiDTO> getReservationsByStatus(String status) throws Exception {
        try {
            final ReservationStatus reservationStatus = ReservationStatus.valueOf(status);
            final GetReservationByStatusRequest request = GetReservationByStatusRequest.newBuilder()
                    .setStatus(reservationStatus)
                    .build();
            Iterable<Reservation> reservationIterable = () -> reservationService.getReservationByStatus(request);
            final List<DemandReservationApiDTO> result = new ArrayList<>();
            for (Reservation reservation : reservationIterable) {
                result.add(reservationMapper.convertReservationToApiDTO(reservation));
            }
            return result;
        } catch (IllegalArgumentException e) {
            logger.error("Illegal argument: " + e.getMessage());
            throw e;
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve reservations: " + e.getMessage());
            throw new OperationFailedException("Failed to retrieve reservations");
        }
    }

}
