package com.vmturbo.plan.orchestrator.reservation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.joda.time.LocalDate;
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
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;

/**
 * Test cases for {@link ReservationRpcService}.
 */
public class ReservationRpcServiceTest {

    private PlanDao planDao;

    private ReservationDao reservationDao;

    private PlanRpcService planRpcService;

    private ReservationRpcService reservationRpcService;

    private ReservationServiceBlockingStub reservationServiceBlockingStub;

    private GrpcTestServer grpcTestServerReservation;

    private Reservation testReservation = Reservation.newBuilder()
            .setId(123)
            .setName("test-reservation")
            .build();

    @Before
    public void setup() throws Exception {
        planDao = Mockito.mock(PlanDao.class);
        reservationDao = Mockito.mock(ReservationDao.class);
        planRpcService = Mockito.mock(PlanRpcService.class);
        reservationRpcService = new ReservationRpcService(planDao, reservationDao, planRpcService);
        grpcTestServerReservation = GrpcTestServer.newServer(reservationRpcService);
        grpcTestServerReservation.start();
        reservationServiceBlockingStub = ReservationServiceGrpc.newBlockingStub(grpcTestServerReservation.getChannel());
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
                                .setTemplateId(123L)))
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
        final LocalDate today = LocalDate.now();
        final LocalDate nextMonth = today.plusMonths(1);
        final CreateReservationRequest request = CreateReservationRequest.newBuilder()
                .setReservation(testReservation)
                .build();
        final Reservation createdReservation = Reservation.newBuilder(testReservation)
                .setId(123)
                .setStartDate(Reservation.Date.newBuilder()
                        .setYear(today.getYear())
                        .setMonth(today.getMonthOfYear())
                        .setDay(today.getDayOfMonth()))
                .setExpirationDate(Reservation.Date.newBuilder()
                        .setYear(nextMonth.getYear())
                        .setMonth(nextMonth.getMonthOfYear())
                        .setDay(nextMonth.getDayOfMonth()))
                .build();
        Mockito.when(reservationDao.createReservation(testReservation)).thenReturn(createdReservation);
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
        Mockito.when(reservationDao.updateReservationBatch(Mockito.anySet()))
                .thenReturn(reservations);
        final Iterator<Reservation> results = reservationServiceBlockingStub.updateReservations(request);
        assertEquals(2L, Iterators.size(results));
    }
}
