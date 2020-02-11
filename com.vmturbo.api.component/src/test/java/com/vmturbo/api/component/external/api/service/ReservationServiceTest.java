package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.enums.ReservationAction.RESERVATION;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

public class ReservationServiceTest {

    private static final int MAXIMUM_PLACEMENT_COUNT = 100;

    private ReservationServiceMole reservationServiceMole =
            Mockito.spy(new ReservationServiceMole());

    private ReservationMapper reservationMapper;

    private PlanServiceMole planServiceMole = Mockito.spy(new PlanServiceMole());

    private ActionsServiceMole actionsServiceMole = Mockito.spy(new ActionsServiceMole());

    private TemplateServiceMole templateServiceMole = Mockito.spy(new TemplateServiceMole());

    private ReservationsService reservationsService;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationServiceMole,
            planServiceMole, actionsServiceMole, templateServiceMole);

    @Before
    public void setup() {
        reservationMapper = Mockito.mock(ReservationMapper.class);
        reservationsService = new ReservationsService(
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()), reservationMapper,
                MAXIMUM_PLACEMENT_COUNT);
    }

    @Test
    public void testCreateReservationForReservation() throws Exception {
        final DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final Reservation reservation = Reservation.newBuilder().setName("Test-reservation").build();
        final DemandReservationApiDTO demandReservationApiDTO = new DemandReservationApiDTO();
        demandReservationApiDTO.setCount(2);
        Mockito.when(reservationMapper.convertToReservation(Mockito.any())).thenReturn(reservation);
        Mockito.when(reservationServiceMole.createReservation(Mockito.any()))
                .thenReturn(reservation);
        Mockito.when(reservationMapper.convertReservationToApiDTO(Mockito.any()))
                .thenReturn(demandReservationApiDTO);
        final DemandReservationApiDTO result =
                reservationsService.createReservationForDemand(false, RESERVATION,
                        demandApiInputDTO);
        Mockito.verify(reservationServiceMole, Mockito.times(1))
                .createReservation(Mockito.any(), Mockito.any());
        assertEquals(2L, (int)result.getCount());
    }

    /**
     * Verify when reservation placement count is larger than maximum allowed placement count, {@link
     * OperationFailedException} exception will be thrown.
     *
     * @throws Exception when failing to create reservation.
     */
    @Test(expected = OperationFailedException.class)
    public void testCreateReservationValidation() throws Exception {
        final DemandReservationApiInputDTO demandApiInputDTO =
                getDemandReservationApiInputDTO(MAXIMUM_PLACEMENT_COUNT + 1);
        reservationsService.createReservationForDemand(false, RESERVATION, demandApiInputDTO);
    }

    private DemandReservationApiInputDTO getDemandReservationApiInputDTO(final int count) {
        final DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final DemandReservationParametersDTO reservationParametersDTO =
                new DemandReservationParametersDTO();
        final PlacementParametersDTO placementParameters = new PlacementParametersDTO();
        placementParameters.setCount(count);
        reservationParametersDTO.setPlacementParameters(placementParameters);
        demandApiInputDTO.setParameters(ImmutableList.of(reservationParametersDTO));
        return demandApiInputDTO;
    }
}
