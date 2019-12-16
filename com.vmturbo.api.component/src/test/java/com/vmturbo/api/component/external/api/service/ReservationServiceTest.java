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
                reservationMapper,
                INITIAL_PLACEMENT_TIMEOUT_SECONDS,
                PlanServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                PlanServiceGrpc.newFutureStub(grpcServer.getChannel()),
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                TemplateServiceGrpc.newBlockingStub(grpcServer.getChannel()));
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
        assertEquals(2L, (int)result.getCount());
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

    @Test
    public void testGetPlacementResults() {
        final long planId = 123L;
        final FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
                .setTopologyContextId(planId)
                .setFilter(ActionQueryFilter.newBuilder()
                        .addTypes(ActionType.ACTIVATE))
                .setPaginationParams(PaginationParameters.getDefaultInstance())
                .build();
        final ActionOrchestratorAction placementMoveAction = ActionOrchestratorAction.newBuilder()
                        .setActionId(1L)
                        .setActionSpec(ActionSpec.newBuilder()
                            .setRecommendation(Action.newBuilder()
                                .setId(90)
                                .setDeprecatedImportance(10.0)
                                .setExplanation(Explanation.newBuilder()
                                    .setMove(MoveExplanation.newBuilder()
                                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                        .setInitialPlacement(
                                            InitialPlacement.getDefaultInstance())))
                                    .build())
                                .setInfo(ActionInfo.newBuilder()
                                    .setMove(Move.newBuilder()
                                        .setTarget(ActionEntity.newBuilder()
                                                .setId(111L)
                                                .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                                        .addChanges(ChangeProvider.newBuilder()
                                            .setSource(ApiUtilsTest.createActionEntity(0L))
                                            .setDestination(ApiUtilsTest.createActionEntity(78910))
                                            .build())
                                        .build())
                                    .build())
                                .build())
                            .build())
                        .build();
        final ActionOrchestratorAction reservationMoveAction = ActionOrchestratorAction.newBuilder()
                .setActionId(2L)
                .setActionSpec(ActionSpec.newBuilder()
                    .setRecommendation(Action.newBuilder()
                        .setId(91)
                        .setDeprecatedImportance(11.0)
                        .setExplanation(Explanation.newBuilder()
                            .setMove(MoveExplanation.newBuilder()
                                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                    .setInitialPlacement(
                                        InitialPlacement.getDefaultInstance())))
                            .build())
                        .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(222L)
                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                                .addChanges(ChangeProvider.newBuilder()
                                    .setSource(ApiUtilsTest.createActionEntity(0L))
                                    .setDestination(ApiUtilsTest.createActionEntity(78911))
                                    .build())
                                .build())
                            .build())
                        .build())
                    .build())
                .build();
        Mockito.when(actionsServiceMole.getAllActions(actionRequest))
            .thenReturn(FilteredActionResponse.newBuilder()
                .addActions(placementMoveAction)
                .addActions(reservationMoveAction)
                .build());
        final GetReservationByStatusRequest reservationRequest = GetReservationByStatusRequest.newBuilder()
                .setStatus(ReservationStatus.RESERVED)
                .build();
        final Reservation reservation = Reservation.newBuilder()
                .setId(333L)
                .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(ReservationTemplate.newBuilder()
                                .addReservationInstance(ReservationInstance.newBuilder()
                                        .setEntityId(222L))))
                .setName("test-reservation")
                .build();
        Mockito.when(reservationServiceMole.getReservationByStatus(reservationRequest))
                .thenReturn(Lists.newArrayList(reservation));
        final Set<Integer> entityTypes = Sets.newHashSet(EntityType.VIRTUAL_MACHINE_VALUE);
        final List<PlacementInfo> placementInfos = reservationsService.getPlacementResults(planId, entityTypes);
        assertEquals(1, placementInfos.size());
        assertEquals(111L, placementInfos.get(0).getEntityId());
        assertEquals(1L, placementInfos.get(0).getProviderIds().size());
        assertTrue(placementInfos.stream()
                .map(PlacementInfo::getProviderIds)
                .flatMap(List::stream)
                .allMatch(providerId -> providerId.equals(78910L)));
    }
}
