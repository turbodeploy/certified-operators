package com.vmturbo.plan.orchestrator.reservation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.CreateReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

/**
 * Test cases for {@link ReservationRpcService}.
 */
public class ReservationRpcServiceTest {
    private final String DISABLE = "DISABLED";

    private PlanDao planDao;

    private TemplatesDao templatesDao;

    private ReservationDao reservationDao;

    private PlanRpcService planRpcService;

    private ReservationManager reservationManager;

    private ReservationRpcService reservationRpcService;

    private ReservationServiceBlockingStub reservationServiceBlockingStub;

    private GrpcTestServer grpcTestServerReservation;

    private Reservation testReservation = Reservation.newBuilder()
            .setId(123)
            .setName("test-reservation")
            .setStatus(ReservationStatus.UNFULFILLED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();

    private Reservation testReservationWithConstraint = Reservation.newBuilder()
            .setId(124)
            .setName("test-reservation-withConstraint")
            .setConstraintInfoCollection(ConstraintInfoCollection.newBuilder()
                    .addReservationConstraintInfo(ReservationConstraintInfo
                            .newBuilder().setConstraintId(123L)))
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();

    @Before
    public void setup() throws Exception {
        templatesDao = Mockito.mock(TemplatesDao.class);
        reservationDao = Mockito.mock(ReservationDao.class);
        reservationManager = Mockito.mock(ReservationManager.class);
        reservationRpcService = new ReservationRpcService(templatesDao,
                reservationDao, reservationManager);
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
                .setApiCallBlock(false)
                .build();
        Optional<Reservation> reservationOptional = Optional.of(testReservation);
        Mockito.when(reservationDao.getReservationById(123, false)).thenReturn(reservationOptional);
        Reservation reservation = reservationServiceBlockingStub.getReservationById(request);
        assertEquals(reservation, reservationOptional.get());
    }

    @Test
    public void testCreateReservation() {
        final Date today = new Date();
        LocalDateTime ldt = today.toInstant()
                        .atOffset(ZoneOffset.UTC).toLocalDateTime();
        final Date nextMonth = Date.from(ldt.plusMonths(1)
                        .atOffset(ZoneOffset.UTC).toInstant());
        final CreateReservationRequest request = CreateReservationRequest.newBuilder()
                .setReservation(testReservation)
                .build();
        final Reservation createdReservation = Reservation.newBuilder(testReservation)
                .setId(123)
                .setStartDate(today.getTime())
                .setExpirationDate(nextMonth.getTime())
                .build();
        Mockito.when(reservationDao.createReservation(testReservation)).thenReturn(createdReservation);
        Mockito.when(reservationManager.intializeReservationStatus(createdReservation)).thenReturn(createdReservation);
        Mockito.when(templatesDao.getTemplatesCount(Mockito.anySet()))
                .thenReturn(1L);
        Reservation reservation = reservationServiceBlockingStub.createReservation(request);
        assertEquals(reservation, createdReservation);
    }

    /**
     * Dont create reservation when the constraint id is invalid.
     */
    @Test
    public void testCreateReservationError() {
        final Date today = new Date();
        LocalDateTime ldt = today.toInstant()
                .atOffset(ZoneOffset.UTC).toLocalDateTime();
        final Date nextMonth = Date.from(ldt.plusMonths(1)
                .atOffset(ZoneOffset.UTC).toInstant());
        final CreateReservationRequest request = CreateReservationRequest.newBuilder()
                .setReservation(testReservationWithConstraint)
                .build();
        Mockito.when(reservationManager.isConstraintIdValid(Mockito.any())).thenReturn(false);
        verify(reservationDao, Mockito.times(0)).createReservation(Mockito.any());
        Mockito.when(templatesDao.getTemplatesCount(Mockito.anySet()))
                .thenReturn(1L);
        try {
            reservationServiceBlockingStub.createReservation(request);
        } catch (Exception e) {
            assertEquals("INVALID_ARGUMENT: constraint Ids are invalid", e.getMessage());
        }
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

}
