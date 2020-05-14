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
     * Clear the ReservationInstance of all the reservationTemplates in the collection.
     *
     * @param reservationTemplateCollection the reservationTemplates collection of interest
     * @return the reservationTemplates collection  with the ReservationInstances cleared.
     */
    public static ReservationTemplateCollection clearProviderFromReservationInstance(
            ReservationTemplateCollection reservationTemplateCollection) {
        ReservationTemplateCollection.Builder updatedReservationTemplateCollection =
                ReservationTemplateCollection.newBuilder();

        for (ReservationTemplate reservationTemplate
                : reservationTemplateCollection.getReservationTemplateList()) {
            ReservationTemplate.Builder updatedReservationTemplate = reservationTemplate.toBuilder().clearReservationInstance();
            for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                ReservationInstance.Builder updatedReservationInstance = reservationInstance.toBuilder().clearPlacementInfo();
                for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                    updatedReservationInstance.addPlacementInfo(placementInfo.toBuilder().clearProviderId());
                }
                updatedReservationTemplate.addReservationInstance(updatedReservationInstance);
            }
            updatedReservationTemplateCollection
                    .addReservationTemplate(updatedReservationTemplate.build());
        }
        return updatedReservationTemplateCollection.build();
    }

    /**
     * change the status of reservation to invalid and clear the ReservationInstance.
     * @param reservation the reservation of interest.
     * @return updated reservation.
     */
    public static Reservation invalidateReservation(Reservation reservation) {
        return reservation.toBuilder()
                .setStatus(ReservationStatus.INVALID)
                .setReservationTemplateCollection(
                        clearProviderFromReservationInstance(
                                reservation.getReservationTemplateCollection()))
                .build();
    }
}

