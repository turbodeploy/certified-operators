package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.enums.ReservationAction;
import com.vmturbo.api.enums.ReservationEditAction;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IReservationsService;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.common.protobuf.plan.ReservationDTO.CreateReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;

/**
 * XL implementation of IReservationAndDeployService.
 **/
public class ReservationsService implements IReservationsService {
    private static final Logger logger = LogManager.getLogger();


    private final ReservationServiceBlockingStub reservationService;

    private final ReservationMapper reservationMapper;


    ReservationsService(@Nonnull final ReservationServiceBlockingStub reservationService,
                               @Nonnull final ReservationMapper reservationMapper) {
        this.reservationService = Objects.requireNonNull(reservationService);
        this.reservationMapper = Objects.requireNonNull(reservationMapper);
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
    public DemandReservationApiDTO getReservationByID(String reservationID) throws Exception {
        try {
            final GetReservationByIdRequest request = GetReservationByIdRequest.newBuilder()
                    .setReservationId(Long.valueOf(reservationID))
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

    @Override
    public DemandReservationApiDTO doActionOnReservationByID(Boolean callBlock,
                                                             ReservationEditAction action, String reservationID) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Boolean deleteReservationByID(String reservationID) {
        final DeleteReservationByIdRequest request = DeleteReservationByIdRequest.newBuilder()
                .setReservationId(Long.valueOf(reservationID))
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
