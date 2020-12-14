package com.vmturbo.components.common.utils;

import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;

/**
 * Util class for updating reservation objects.
 */
public class ReservationProtoUtil {

    /**
     * constructor.
     */
    private ReservationProtoUtil() {
        throw new UnsupportedOperationException();
    }

    /**
     * Clear the ReservationInstance of the reservation.
     *
     * @param reservation      the reservation of interest.
     * @param clearProvider    clare providerInfo if true
     * @param clearFailureInfo clear the failure info if true
     * @return reservation with  ReservationInstance  cleared appropriately.
     */
    public static Reservation clearReservationInstance(
            Reservation reservation,
            boolean clearProvider,
            boolean clearFailureInfo) {
        ReservationTemplateCollection.Builder updatedReservationTemplateCollection =
                ReservationTemplateCollection.newBuilder();

        for (ReservationTemplate reservationTemplate
                : reservation.getReservationTemplateCollection().getReservationTemplateList()) {
            ReservationTemplate.Builder updatedReservationTemplate = reservationTemplate.toBuilder().clearReservationInstance();
            for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                ReservationInstance.Builder updatedReservationInstance = reservationInstance.toBuilder();
                if (clearProvider) {
                    updatedReservationInstance.clearPlacementInfo();
                    for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                        updatedReservationInstance.addPlacementInfo(placementInfo
                                .toBuilder()
                                .clearProviderId()
                                .clearClusterId()
                                .clearCommodityStats());
                    }
                }
                if (clearFailureInfo) {
                    updatedReservationInstance.clearUnplacedReason();
                }
                updatedReservationTemplate.addReservationInstance(updatedReservationInstance);
            }
            updatedReservationTemplateCollection
                    .addReservationTemplate(updatedReservationTemplate.build());
        }
        return reservation.toBuilder()
                .setReservationTemplateCollection(updatedReservationTemplateCollection).build();
    }

    /**
     * change the status of reservation to invalid and clear the ReservationInstance.
     *
     * @param reservation the reservation of interest.
     * @return updated reservation.
     */
    public static Reservation invalidateReservation(Reservation reservation) {
        Reservation updatedReservation = clearReservationInstance(reservation,
                true, true);
        return updatedReservation.toBuilder().setStatus(ReservationStatus.INVALID)
                .build();
    }
}

