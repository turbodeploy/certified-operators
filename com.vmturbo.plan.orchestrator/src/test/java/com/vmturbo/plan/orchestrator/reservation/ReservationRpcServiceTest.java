package com.vmturbo.plan.orchestrator.reservation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.CreateReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test cases for {@link ReservationRpcService}.
 */
public class ReservationRpcServiceTest {
    private final String DISABLE = "DISABLED";

    private PlanDao planDao;

    private TemplatesDao templatesDao;

    private ReservationDao reservationDao;

    private PlanRpcService planRpcService;

    private ReservationRpcService reservationRpcService;

    private ReservationServiceBlockingStub reservationServiceBlockingStub;

    private GrpcTestServer grpcTestServerReservation;

    private Reservation testReservation = Reservation.newBuilder()
            .setId(123)
            .setName("test-reservation")
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();

    @Before
    public void setup() throws Exception {
        planDao = Mockito.mock(PlanDao.class);
        templatesDao = Mockito.mock(TemplatesDao.class);
        reservationDao = Mockito.mock(ReservationDao.class);
        planRpcService = Mockito.mock(PlanRpcService.class);
        reservationRpcService = new ReservationRpcService(planDao, templatesDao, reservationDao,
                planRpcService);
        grpcTestServerReservation = GrpcTestServer.newServer(reservationRpcService);
        grpcTestServerReservation.start();
        reservationServiceBlockingStub =
                ReservationServiceGrpc.newBlockingStub(grpcTestServerReservation.getChannel());
    }

    @After
    public void tearDown() {
        grpcTestServerReservation.close();
    }

    @Test
    public void testInitialPlacement() throws Exception {
        final ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .addChanges(ScenarioChange.newBuilder()
                        .setTopologyAddition(TopologyAddition.newBuilder()
                                .setTemplateId(234L)))
                .build();
        final InitialPlacementRequest request = InitialPlacementRequest.newBuilder()
                .setScenarioInfo(scenarioInfo)
                .build();

        final StreamObserver<InitialPlacementResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(1L)
                .setStatus(PlanStatus.RUNNING_ANALYSIS)
                .build();
        Mockito.when(planDao.createPlanInstance(Mockito.any(), Mockito.any())).thenReturn(planInstance);
        Mockito.when(templatesDao.getTemplatesCount(Mockito.anySet()))
                .thenReturn(1L);
        reservationServiceBlockingStub.initialPlacement(request);

        Mockito.verify(planDao, Mockito.times(1))
                .createPlanInstance(Mockito.any(), Mockito.any());
        Mockito.verify(planRpcService, Mockito.times(1))
                .runPlan(Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetAllReservation() {
        final GetAllReservationsRequest request = GetAllReservationsRequest.newBuilder().build();
        Set<Reservation> reservationSet = Sets.newHashSet(testReservation);
        Mockito.when(reservationDao.getAllReservations()).thenReturn(reservationSet);
        Iterator<Reservation> reservations = reservationServiceBlockingStub.getAllReservations(request);
        assertTrue(reservations.hasNext());
        assertEquals(reservationSet, Sets.newHashSet(reservations));
    }

    @Test
    public void testGetReservationById() {
        final GetReservationByIdRequest request = GetReservationByIdRequest.newBuilder()
                .setReservationId(123)
                .build();
        Optional<Reservation> reservationOptional = Optional.of(testReservation);
        Mockito.when(reservationDao.getReservationById(123)).thenReturn(reservationOptional);
        Reservation reservation = reservationServiceBlockingStub.getReservationById(request);
        assertEquals(reservation, reservationOptional.get());
    }

    @Test
    public void testCreateReservation() {
        final DateTime today = DateTime.now(DateTimeZone.UTC);
        final DateTime nextMonth = today.plusMonths(1);
        final CreateReservationRequest request = CreateReservationRequest.newBuilder()
                .setReservation(testReservation)
                .build();
        final Reservation createdReservation = Reservation.newBuilder(testReservation)
                .setId(123)
                .setStartDate(today.getMillis())
                .setExpirationDate(nextMonth.getMillis())
                .build();
        Mockito.when(reservationDao.createReservation(testReservation)).thenReturn(createdReservation);
        Mockito.when(templatesDao.getTemplatesCount(Mockito.anySet()))
                .thenReturn(1L);
        Reservation reservation = reservationServiceBlockingStub.createReservation(request);
        assertEquals(reservation, createdReservation);
    }

    @Test
    public void testUpdateReservationById() throws NoSuchObjectException {
        final Reservation newReservation = Reservation.newBuilder(testReservation)
                .setName("new-reservation")
                .build();
        final UpdateReservationByIdRequest request = UpdateReservationByIdRequest.newBuilder()
                .setReservationId(123)
                .setReservation(newReservation)
                .build();
        Mockito.when(reservationDao.updateReservation(123, newReservation)).thenReturn(newReservation);
        Mockito.when(templatesDao.getTemplatesCount(Mockito.anySet())).thenReturn(1L);
        Reservation reservation = reservationServiceBlockingStub.updateReservationById(request);
        assertEquals(reservation, newReservation);
    }

    @Test
    public void testDeleteReservation() throws NoSuchObjectException {
        final DeleteReservationByIdRequest request = DeleteReservationByIdRequest.newBuilder()
                .setReservationId(123)
                .build();
        Mockito.when(reservationDao.deleteReservationById(123)).thenReturn(testReservation);
        Reservation reservation = reservationServiceBlockingStub.deleteReservationById(request);
        assertEquals(reservation, testReservation);
    }

    @Test
    public void testGetReservationsByStatus() {
        final Reservation newReservation = Reservation.newBuilder(testReservation)
                .setName("new-reservation")
                .setStatus(ReservationStatus.FUTURE)
                .build();
        Mockito.when(reservationDao.getReservationsByStatus(ReservationStatus.FUTURE))
                .thenReturn(Sets.newHashSet(newReservation));
        final GetReservationByStatusRequest request = GetReservationByStatusRequest.newBuilder()
                .setStatus(ReservationStatus.FUTURE)
                .build();
        Iterator<Reservation> reservation = reservationServiceBlockingStub.getReservationByStatus(request);
        assertTrue(reservation.hasNext());
        assertEquals(reservation.next(), newReservation);
    }

    @Test
    public void testUpdateReservations() throws Exception {
        final Reservation firstReservation = Reservation.newBuilder(testReservation)
                .setId(123)
                .setName("first-reservation")
                .build();
        final Reservation secondReservation = Reservation.newBuilder(testReservation)
                .setId(456)
                .setName("second-reservation")
                .build();
        final Set<Reservation> reservations = Sets.newHashSet(firstReservation, secondReservation);
        final UpdateReservationsRequest request = UpdateReservationsRequest.newBuilder()
                .addAllReservation(reservations)
                .build();
        Mockito.when(templatesDao.getTemplatesCount(Mockito.anySet()))
                .thenReturn(1L);
        Mockito.when(reservationDao.updateReservationBatch(Mockito.anySet()))
                .thenReturn(reservations);
        final Iterator<Reservation> results = reservationServiceBlockingStub.updateReservations(request);
        assertEquals(2L, Iterators.size(results));
    }

    @Test
    public void testPlacementSettingOverride() {
        final List<ScenarioChange> scenarioChangeList =
                reservationRpcService.createPlacementActionSettingOverride();
        assertEquals(4, scenarioChangeList.size());
        final Optional<Setting> moveSettingForVm = scenarioChangeList.stream()
                .map(ScenarioChange::getSettingOverride)
                .filter(settingOverride -> settingOverride.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .map(SettingOverride::getSetting)
                .filter(setting ->
                        setting.getSettingSpecName().equals(EntitySettingSpecs.Move.getSettingName()))
                .findFirst();
        final Optional<Setting> movesSettingForSt = scenarioChangeList.stream()
                .map(ScenarioChange::getSettingOverride)
                .filter(settingOverride -> settingOverride.getEntityType() == EntityType.STORAGE_VALUE)
                .map(SettingOverride::getSetting)
                .filter(setting ->
                        setting.getSettingSpecName().equals(EntitySettingSpecs.Move.getSettingName()))
                .findFirst();
        final Optional<Setting> provisionSetting = scenarioChangeList.stream()
                .map(ScenarioChange::getSettingOverride)
                .map(SettingOverride::getSetting)
                .filter(setting ->
                        setting.getSettingSpecName().equals(EntitySettingSpecs.Provision.getSettingName()))
                .findFirst();
        final Optional<Setting> storageSetting = scenarioChangeList.stream()
                .map(ScenarioChange::getSettingOverride)
                .filter(settingOverride -> settingOverride.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .map(SettingOverride::getSetting)
                .filter(setting ->
                        setting.getSettingSpecName().equals(EntitySettingSpecs.StorageMove.getSettingName()))
                .findFirst();
        assertTrue(moveSettingForVm.isPresent());
        assertTrue(moveSettingForVm.get().getEnumSettingValue().getValue().equals(DISABLE));
        assertTrue(movesSettingForSt.isPresent());
        assertTrue(movesSettingForSt.get().getEnumSettingValue().getValue().equals(DISABLE));
        assertTrue(provisionSetting.isPresent());
        assertTrue(provisionSetting.get().getEnumSettingValue().getValue().equals(DISABLE));
        assertTrue(storageSetting.isPresent());
        assertTrue(storageSetting.get().getEnumSettingValue().getValue().equals(DISABLE));
    }
}
