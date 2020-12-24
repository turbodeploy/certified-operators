package com.vmturbo.topology.processor.reservation;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateFutureAndExpiredReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateFutureAndExpiredReservationsResponse;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * Check if there are any Reservation should become active (start day is today or before and
 * status is FUTURE) and change its status and send request to update Reservation table.
 */
public class ReservationManager {
    private static final Logger logger = LogManager.getLogger();
    /**
     * Track reservation counts every broadcast.
     */
    private static final DataMetricGauge RESERVATION_STATUS_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "current_reservations")
            .withHelp("Reservation per status each broadcast")
            .withLabelNames("status")
            .build()
            .register();

    private final ReservationServiceBlockingStub reservationService;

    private final ReservationServiceStub reservationServiceNonBlocking;

    /**
     * Constructor for ReservationManager.
     * @param reservationService blocking Stub
     * @param reservationServiceNonBlocking non blocking stub
     */
    ReservationManager(@Nonnull final ReservationServiceBlockingStub reservationService,
                       @Nonnull final ReservationServiceStub reservationServiceNonBlocking) {
        this.reservationService = Objects.requireNonNull(reservationService);
        this.reservationServiceNonBlocking = Objects.requireNonNull(reservationServiceNonBlocking);
    }

    /**
     * Convert all active and potential active Reservations to TopologyEntity and add them into live
     * topology.
     *
     * @param topologyType type of the topology under construction.
     * @return The number of reservation entities
     */
    public Status applyReservation(TopologyType topologyType) {
        RESERVATION_STATUS_GAUGE.getLabeledMetrics().forEach((key, val) -> {
            val.setData(0.0);
        });
        reservationService.getAllReservations(GetAllReservationsRequest.getDefaultInstance())
                .forEachRemaining(reservation -> {
                    RESERVATION_STATUS_GAUGE.labels(reservation.getStatus().toString()).increment();
                });

        // update the future, expired and invalid reservations.
        if (topologyType == TopologyType.REALTIME) {
            StreamObserver<UpdateFutureAndExpiredReservationsResponse> response =
                    new StreamObserver<UpdateFutureAndExpiredReservationsResponse>() {

                        @Override
                        public void onNext(UpdateFutureAndExpiredReservationsResponse
                                                   updateFutureAndExpiredReservationsResponse) {
                        }

                        @Override
                        public void onError(Throwable throwable) {
                        }

                        @Override
                        public void onCompleted() {
                        }
                    };
            UpdateFutureAndExpiredReservationsRequest request =
                    UpdateFutureAndExpiredReservationsRequest.newBuilder()
                    .build();
            reservationServiceNonBlocking.updateFutureAndExpiredReservations(request,
                    response);
        }
        return Status.success();
    }
}