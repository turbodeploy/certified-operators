package com.vmturbo.api.component.external.api.service;

import java.util.List;
import java.util.Map;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.serviceinterfaces.IReservationsService;
import com.vmturbo.api.enums.ReservationAction;
import com.vmturbo.api.enums.ReservationEditAction;

/**
 * XL implementation of IReservationAndDeployService
 **/
public class ReservationsService implements IReservationsService {

    @Override
    public List<DemandReservationApiDTO> getAllReservations(Map<String, String> queryParams)
                    throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DemandReservationApiDTO getReservationByID(String reservationID) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DemandReservationApiDTO createReservationForDemand(Boolean apiCallBlock,
                                                              ReservationAction demandAction,
                    DemandReservationApiInputDTO demandApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DemandReservationApiDTO doActionOnReservationByID(Boolean callBlock,
                                                             ReservationEditAction action, String reservationID) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public Boolean deleteReservationByID(String reservationID) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public DemandReservationApiDTO deployReservationByID(Boolean callBlock, String reservationID)
                    throws Exception {
        throw ApiUtils.notImplementedInXL();
    }


}
