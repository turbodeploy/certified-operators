package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.enums.ReservationAction.PLACEMENT;
import static com.vmturbo.api.enums.ReservationAction.RESERVATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.component.external.api.mapper.ReservationMapper.PlacementInfo;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles.TemplateServiceMole;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ReservationServiceTest {

    private ReservationServiceMole reservationServiceMole =
            Mockito.spy(new ReservationServiceMole());

    private ReservationMapper reservationMapper;

    private PlanServiceMole planServiceMole = Mockito.spy(new PlanServiceMole());

    private ActionsServiceMole actionsServiceMole = Mockito.spy(new ActionsServiceMole());

    private TemplateServiceMole templateServiceMole = Mockito.spy(new TemplateServiceMole());

    private ReservationsService reservationsService;

    private final long INITIAL_PLACEMENT_TIMEOUT_SECONDS = 600;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationServiceMole,
            planServiceMole, actionsServiceMole, templateServiceMole);

    @Before
    public void setup() {
        reservationMapper = Mockito.mock(ReservationMapper.class);
        reservationsService = new ReservationsService(
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                reservationMapper);
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
        assertEquals(2L, (int)result.getCount());
    }

}
