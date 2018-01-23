package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.enums.ReservationAction.PLACEMENT;
import static com.vmturbo.api.enums.ReservationAction.RESERVATION;

import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

public class ReservationServiceTest {

    private ReservationServiceMole reservationServiceMole =
            Mockito.spy(new ReservationServiceMole());

    private ReservationMapper reservationMapper;

    private PlanServiceMole planServiceMole = Mockito.spy(new PlanServiceMole());

    private ActionsServiceMole actionsServiceMole = Mockito.spy(new ActionsServiceMole());

    private ReservationsService reservationsService;

    private final long INITIAL_PLACEMENT_TIMEOUT_SECONDS = 600;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationServiceMole,
            planServiceMole, actionsServiceMole);

    @Before
    public void setup() {
        reservationMapper = Mockito.mock(ReservationMapper.class);
        reservationsService = new ReservationsService(
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                reservationMapper,
                INITIAL_PLACEMENT_TIMEOUT_SECONDS,
                PlanServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                PlanServiceGrpc.newFutureStub(grpcServer.getChannel()),
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    @Test
    public void testCreateReservationForDemand() throws Exception {
        final DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final DemandReservationApiDTO demandReservationApiDTO = new DemandReservationApiDTO();
        demandReservationApiDTO.setCount(2);
        final ScenarioChange scenarioChange = ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(1)
                        .setTemplateId(222L))
                .build();

        Mockito.when(reservationMapper.placementToScenarioChange(Mockito.any()))
                .thenReturn(Lists.newArrayList(scenarioChange));

        Mockito.when(reservationServiceMole.initialPlacement(InitialPlacementRequest.newBuilder()
                .setScenarioInfo(ScenarioInfo.newBuilder()
                        .addChanges(scenarioChange))
                .build()))
                .thenReturn(InitialPlacementResponse.newBuilder()
                        .setPlanId(123L)
                        .build());
        Mockito.when(planServiceMole.getPlan(PlanId.newBuilder()
                .setPlanId(123L)
                .build()))
                .thenReturn(OptionalPlanInstance.newBuilder()
                        .setPlanInstance(PlanInstance.newBuilder()
                                .setPlanId(123L)
                                .setStatus(PlanStatus.SUCCEEDED))
                        .build());
        Mockito.when(reservationMapper.convertToDemandReservationApiDTO(Mockito.any(),
                Mockito.any()))
                .thenReturn(demandReservationApiDTO);
        final DemandReservationApiDTO result =
                reservationsService.createReservationForDemand(false, PLACEMENT,
                        demandApiInputDTO);
        Mockito.verify(reservationServiceMole, Mockito.times(1))
                .initialPlacement(InitialPlacementRequest.newBuilder()
                        .setScenarioInfo(ScenarioInfo.newBuilder()
                                .addChanges(scenarioChange))
                        .build());
        Mockito.verify(planServiceMole, Mockito.times(1))
                .getPlan(PlanId.newBuilder()
                        .setPlanId(123L)
                        .build());
        Assert.assertEquals(2L, (int)result.getCount());
    }

    @Test
    public void testCreateReservationForReservation() throws Exception {
        final DemandReservationApiInputDTO demandApiInputDTO = new DemandReservationApiInputDTO();
        final Reservation reservation = Reservation.newBuilder()
                .setName("Test-reservation")
                .build();
        final DemandReservationApiDTO demandReservationApiDTO = new DemandReservationApiDTO();
        demandReservationApiDTO.setCount(2);
        Mockito.when(reservationMapper.convertToReservation(Mockito.any()))
                .thenReturn(reservation);
        Mockito.when(reservationServiceMole.createReservation(Mockito.any()))
                .thenReturn(reservation);
        Mockito.when(reservationMapper.convertReservationToApiDTO(Mockito.any()))
                .thenReturn(demandReservationApiDTO);
        final DemandReservationApiDTO result =
                reservationsService.createReservationForDemand(false, RESERVATION,
                        demandApiInputDTO);
        Mockito.verify(reservationServiceMole, Mockito.times(1))
                .createReservation(Mockito.any(), Mockito.any());
        Assert.assertEquals(2L, (int)result.getCount());
    }
}
