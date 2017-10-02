package com.vmturbo.api.component.external.api.service;

import java.util.List;
import java.util.Map;

import com.vmturbo.api.dto.input.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.serviceinterfaces.IReservationsService;
import com.vmturbo.api.utils.ParamStrings.RESERVATION_POST_API_ACTION;
import com.vmturbo.api.utils.ParamStrings.RESERVATION_PUT_API_ACTION;

/**
 * XL implementation of IReservationAndDeployService
 **/
public class ReservationsService implements IReservationsService {

    @Override
    public List<DemandReservationApiDTO> getAllReservations(Map<String, String> queryParams)
                    throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DemandReservationApiDTO getReservationByID(String reservationID) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DemandReservationApiDTO createReservationForDemand(Boolean apiCallBlock,
                    RESERVATION_POST_API_ACTION demandAction,
                    DemandReservationApiInputDTO demandApiInputDTO) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DemandReservationApiDTO doActionOnReservationByID(Boolean callBlock,
                    RESERVATION_PUT_API_ACTION action, String reservationID) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Boolean deleteReservationByID(String reservationID) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DemandReservationApiDTO deployReservationByID(Boolean callBlock, String reservationID)
                    throws Exception {
        // TODO Auto-generated method stub
        return null;
    }


}
