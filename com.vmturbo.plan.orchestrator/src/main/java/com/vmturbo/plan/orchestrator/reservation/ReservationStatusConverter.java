package com.vmturbo.plan.orchestrator.reservation;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ReservationDTO;

/**
 * A converter between dbStatus int and {@link ReservationDTO.ReservationStatus}.
 */
public class ReservationStatusConverter {

    @Nonnull
    public static int typeToDb(@Nonnull final ReservationDTO.ReservationStatus status) {
        switch (status) {
            case FUTURE:
                return 1;
            case RESERVED:
                return 2;
            case INVALID:
                return 3;
            case UNFULFILLED:
                return 4;
            case INPROGRESS:
                return 5;
            // reservation was part of  reservation plan and there is not enough space currently
            case PLACEMENT_FAILED:
                return 6;
            case INITIAL:
                return 7;
            default:
                throw new IllegalArgumentException("Unexpected status: " + status);
        }
    }

    @Nonnull
    static ReservationDTO.ReservationStatus typeFromDb(@Nonnull final int dbStatus) {
        switch (dbStatus) {
            case 1:
                return ReservationDTO.ReservationStatus.FUTURE;
            case 2:
                return ReservationDTO.ReservationStatus.RESERVED;
            case 3:
                return ReservationDTO.ReservationStatus.INVALID;
            case 4:
                return ReservationDTO.ReservationStatus.UNFULFILLED;
            case 5:
                return ReservationDTO.ReservationStatus.INPROGRESS;
            case 6:
                return ReservationDTO.ReservationStatus.PLACEMENT_FAILED;
            case 7:
                return ReservationDTO.ReservationStatus.INITIAL;
            default:
                throw new IllegalArgumentException("Unexpected status: " + dbStatus);
        }
    }
}
