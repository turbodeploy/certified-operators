package com.vmturbo.plan.orchestrator.reservation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;

/**
 * Test cases for {@link ReservationManager}.
 */
public class ReservationManagerTest {

    private PlanDao planDao;

    private ReservationDao reservationDao;

    private PlanRpcService planRpcService;

    private ReservationManager reservationManager;

    @Captor
    private ArgumentCaptor<Set<Reservation>> updateBatchCaptor;

    private Reservation testReservation = Reservation.newBuilder()
            .setId(1000)
            .setName("test-reservation")
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();

    // active 100 hrs later expires 101 hrs later
    private Reservation testFutureReservation = Reservation.newBuilder()
            .setId(1001)
            .setName("test-reservation")
            .setStartDate(System.currentTimeMillis() + 1000 * 60 * 60 * 100)
            .setExpirationDate(System.currentTimeMillis() + 1000 * 60 * 60 * 101)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();


    private Reservation unfulfilledReservation = Reservation.newBuilder()
            .setId(1002)
            .setName("test-reservation")
            .setStatus(ReservationStatus.UNFULFILLED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();

    private Reservation inProgressReservation1 = Reservation.newBuilder()
            .setId(1003)
            .setName("test-reservation")
            .setStatus(ReservationStatus.INPROGRESS)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)
                            .addReservationInstance(ReservationInstance.newBuilder())))
            .build();

    private Reservation inProgressReservation2 = Reservation.newBuilder()
            .setId(1004)
            .setName("test-reservation")
            .setStatus(ReservationStatus.INPROGRESS)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)
                            .addReservationInstance(ReservationInstance.newBuilder()
                                    .addPlacementInfo(PlacementInfo.newBuilder()))))
            .build();

    private Reservation inProgressReservation3 = Reservation.newBuilder()
            .setId(1005)
            .setName("test-reservation")
            .setStatus(ReservationStatus.INPROGRESS)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)
                            .addReservationInstance(ReservationInstance.newBuilder()
                                    .addPlacementInfo(PlacementInfo.newBuilder().setProviderId(500L)))))
            .build();


    // expired
    private Reservation testExpiredReservation = Reservation.newBuilder()
            .setId(1006)
            .setName("test-reservation")
            .setStartDate(System.currentTimeMillis() - 2000)
            .setExpirationDate(System.currentTimeMillis() - 1000)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();

    // active for the last 2 hrs and expires in another hr
    private Reservation testActiveNotExpiredReservation = Reservation.newBuilder()
            .setId(1007)
            .setName("test-reservation")
            .setStartDate(System.currentTimeMillis() - 1000 * 60 * 60 * 2)
            .setExpirationDate(System.currentTimeMillis() + 1000 * 60 * 60)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplateId(234L)))
            .build();

    /**
     * Initial setup.
     * @throws Exception because of calls to reservationDao methods.
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        planDao = Mockito.mock(PlanDao.class);
        reservationDao = Mockito.mock(ReservationDao.class);
        planRpcService = Mockito.mock(PlanRpcService.class);
        reservationManager = new ReservationManager(planDao, reservationDao, planRpcService);
    }

    /**
     * Test intializeReservationStatus method with future reservation.
     */
    @Test
    public void testIntializeFutureReservationStatus() {
        Reservation queuedReservation =
                reservationManager.intializeReservationStatus(testFutureReservation);
        assert (queuedReservation.getStatus().equals(ReservationStatus.FUTURE));
    }

    /**
     * Test intializeReservationStatus method with current reservation.
     */
    @Test
    public void testIntializeTodayReservationStatus() {
        Reservation queuedReservation =
                reservationManager.intializeReservationStatus(testReservation);
        assert (queuedReservation.getStatus().equals(ReservationStatus.UNFULFILLED));
    }

    /**
     * Test intializeReservationStatus method with current reservation.
     */
    @Test
    public void testHasReservationExpired() {
        assertFalse(reservationManager.hasReservationExpired(testFutureReservation));
        assertFalse(reservationManager.hasReservationExpired(testActiveNotExpiredReservation));
        assertTrue(reservationManager.hasReservationExpired(testExpiredReservation));
    }

    /**
     * Test checkAndStartReservationPlan method with no in progress reservation.
     */
    @Test
    public void testCheckAndStartReservationPlanSuccess() {
        ReservationManager reservationManagerSpy = spy(reservationManager);
        Mockito.doNothing().when(reservationManagerSpy).runPlanForBatchReservation();
        when(reservationDao.getAllReservations())
                .thenReturn(new HashSet<>(Arrays.asList(unfulfilledReservation)));
        reservationManagerSpy.checkAndStartReservationPlan();
        try {
            verify(reservationDao, times(1)).updateReservationBatch(updateBatchCaptor.capture());
            Set<Reservation> updatedReservations = updateBatchCaptor.getValue();
            assertThat(updatedReservations, containsInAnyOrder(unfulfilledReservation.toBuilder()
                .setStatus(ReservationStatus.INPROGRESS)
                .build()));
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test checkAndStartReservationPlan method with in progress reservation.
     */
    @Test
    public void testCheckAndStartReservationPlanFailure() {
        ReservationManager reservationManagerSpy = spy(reservationManager);
        Mockito.doNothing().when(reservationManagerSpy).runPlanForBatchReservation();
        when(reservationDao.getAllReservations())
                .thenReturn(new HashSet<>(Arrays.asList(inProgressReservation1, unfulfilledReservation)));
        reservationManagerSpy.checkAndStartReservationPlan();

        when(reservationDao.getAllReservations())
                .thenReturn(new HashSet<>(Arrays.asList(inProgressReservation1)));
        reservationManagerSpy.checkAndStartReservationPlan();
        try {
            verify(reservationDao, times(0)).updateReservationBatch(anySet());
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test updateReservationResult method with 3 in progress reservations.
     * Only one reservation is successfully reserved. One reservations does not have
     * placement info at all. Another reservation has placement info but the
     * provider info is absent.
     */
    @Test
    public void testUpdateReservationResult() {
        HashSet<Reservation> inProgressReservation =
                new HashSet<>(Arrays.asList(inProgressReservation1,
                        inProgressReservation2,
                        inProgressReservation3));
        ArgumentCaptor<HashSet<Reservation>> captor =
                ArgumentCaptor.forClass((Class<HashSet<Reservation>>)(Class)HashSet
                        .class);
        reservationManager.updateReservationResult(inProgressReservation);
        try {
            verify(reservationDao, times(1)).updateReservationBatch(captor.capture());
            Set<Long> reservedReservations =
                    captor.getValue().stream()
                            .filter(a -> a.getStatus() == ReservationStatus.RESERVED)
                            .map(a -> a.getId())
                            .collect(Collectors.toSet());
            Set<Long> failedReservations =
                    captor.getValue().stream()
                            .filter(a -> a.getStatus() == ReservationStatus.PLACEMENT_FAILED)
                            .map(a -> a.getId())
                            .collect(Collectors.toSet());

            assert (reservedReservations.contains(1005L));
            assert (failedReservations.contains(1003L));
            assert (failedReservations.contains(1004L));
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test that a reservation plan failure marks in-progress reservations as invalid.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPlanFailure() throws Exception {
        when(reservationDao.getAllReservations())
            .thenReturn(Sets.newHashSet(inProgressReservation1, unfulfilledReservation));

        reservationManager.onPlanStatusChanged(PlanInstance.newBuilder()
            .setPlanId(1)
            .setProjectType(PlanProjectType.RESERVATION_PLAN)
            .setStatus(PlanStatus.FAILED)
            .build());

        verify(reservationDao).updateReservationBatch(updateBatchCaptor.capture());

        final Set<Reservation> updatedReservations = updateBatchCaptor.getValue();

        // We should have updated the in-progress reservation to invalid.
        assertThat(updatedReservations, containsInAnyOrder(inProgressReservation1.toBuilder()
            .setStatus(ReservationStatus.INVALID)
            .build()));
    }

}
