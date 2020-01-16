package com.vmturbo.plan.orchestrator.reservation;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.plan.orchestrator.db.enums.ReservationStatus;

/**
 * A converter between {@link ReservationStatus} with {@link ReservationDTO.ReservationStatus}.
 */
public class ReservationStatusConverter {

    @Nonnull
    static ReservationStatus typeToDb(@Nonnull final ReservationDTO.ReservationStatus status) {
        switch (status) {
            case FUTURE:
                return ReservationStatus.future;
            case RESERVED:
                return ReservationStatus.reserved;
            case INVALID:
                return ReservationStatus.invalid;
            default:
                throw new IllegalArgumentException("Unexpected status: " + status);
        }
    }

    @Nonnull
    static ReservationDTO.ReservationStatus typeFromDb(@Nonnull final ReservationStatus dbStatus) {
        switch (dbStatus) {
            case future:
                return ReservationDTO.ReservationStatus.FUTURE;
            case reserved:
                return ReservationDTO.ReservationStatus.RESERVED;
            case invalid:
                return ReservationDTO.ReservationStatus.INVALID;
            default:
                throw new IllegalArgumentException("Unexpected status: " + dbStatus);
        }
    }
}
