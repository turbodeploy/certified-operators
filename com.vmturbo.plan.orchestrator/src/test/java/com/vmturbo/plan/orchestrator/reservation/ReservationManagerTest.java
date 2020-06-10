package com.vmturbo.plan.orchestrator.reservation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
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
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyerPlacementInfo;
import com.vmturbo.common.protobuf.market.InitialPlacementMoles.InitialPlacementServiceMole;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

/**
 * Test cases for {@link ReservationManager}.
 */
public class ReservationManagerTest {

    private PlanDao planDao;

    private ReservationDao reservationDao;

    private PlanRpcService planRpcService;

    private ReservationManager reservationManager;

    private IMessageSender<ReservationDTO.ReservationChanges> sender;

    private ReservationNotificationSender resNotificationSender;


    private InitialPlacementServiceMole initialPlacementService = spy(new InitialPlacementServiceMole());

    /**
     * mock server for initialPlacementService.
     */
    @Rule
    public GrpcTestServer mockServer = GrpcTestServer.newServer(initialPlacementService);

    private InitialPlacementServiceBlockingStub initialPlacementServiceBlockingStub;

    private TemplatesDao templatesDao;

    @Captor
    private ArgumentCaptor<Set<Reservation>> updateBatchCaptor;

    private Template template = Template.newBuilder()
            .setId(234L)
            .setTemplateInfo(TemplateInfo.newBuilder()
                    .setName("templateName")
                    .addResources(TemplateResource.newBuilder()
                            .addFields(TemplateField.newBuilder().setValue("10").setName("diskSize"))
                            .addFields(TemplateField.newBuilder().setValue("20").setName("diskSize"))
                            .addFields(TemplateField.newBuilder().setValue("30").setName("diskSize"))
                            .addFields(TemplateField.newBuilder().setValue("40").setName("cpuSpeed"))
                            .addFields(TemplateField.newBuilder().setValue("50").setName("memorySize"))
                            .addFields(TemplateField.newBuilder().setValue("3").setName("numOfCores"))
                    ))
            .build();

    private Reservation testReservation = Reservation.newBuilder()
            .setId(1000)
            .setName("test-reservation")
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplate(template)))
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
                            .setTemplate(template)))
            .build();


    private Reservation unfulfilledReservation = Reservation.newBuilder()
            .setId(1002)
            .setName("test-reservation")
            .setStatus(ReservationStatus.UNFULFILLED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplate(template)))
            .build();

    private Reservation inProgressReservation1 = Reservation.newBuilder()
            .setId(1003)
            .setName("test-reservation")
            .setStatus(ReservationStatus.INPROGRESS)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplate(template)
                            .addReservationInstance(ReservationInstance.newBuilder())))
            .build();

    private Reservation fastReservation = Reservation.newBuilder()
            .setId(10000)
            .setName("fast-reservation")
            .setStatus(ReservationStatus.INPROGRESS)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(2L)
                            .setTemplate(template)))
            .build();



    private Reservation inProgressReservation2 = Reservation.newBuilder()
            .setId(1004)
            .setName("test-reservation")
            .setStatus(ReservationStatus.INPROGRESS)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(1L)
                            .setTemplate(template)
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
                            .setTemplate(template)
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
                            .setTemplate(template)))
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
                            .setTemplate(template)))
            .build();

    private Reservation testReservationForBroadcast1 = Reservation.newBuilder()
            .setId(1008)
            .setName("test-reservation1")
            .setStatus(ReservationStatus.RESERVED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(10L)
                            .setTemplate(template)))
            .build();

    private Reservation testReservationForBroadcast2 = Reservation.newBuilder()
            .setId(1009)
            .setName("test-reservation2")
            .setStatus(ReservationStatus.PLACEMENT_FAILED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setCount(20L)
                            .setTemplate(template)))
            .build();

    /**
     * Initial setup.
     *
     * @throws Exception because of calls to reservationDao methods.
     */
    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        planDao = Mockito.mock(PlanDao.class);
        reservationDao = Mockito.mock(ReservationDao.class);
        planRpcService = Mockito.mock(PlanRpcService.class);
        initialPlacementServiceBlockingStub = InitialPlacementServiceGrpc.newBlockingStub(mockServer.getChannel());
        templatesDao = Mockito.mock(TemplatesDao.class);
        sender = Mockito.mock(IMessageSender.class);
        resNotificationSender = new ReservationNotificationSender(sender);

        reservationManager = new ReservationManager(planDao, reservationDao, planRpcService, resNotificationSender, initialPlacementServiceBlockingStub, templatesDao);
    }

    /**
     * Test intializeReservationStatus method with future reservation.
     */
    @Test
    public void testIntializeFutureReservationStatus() {
        ReservationManager reservationManagerSpy = spy(reservationManager);
        Mockito.doReturn(testFutureReservation).when(reservationManagerSpy).addEntityToReservation(Matchers.any());
        Reservation queuedReservation =
                reservationManagerSpy.intializeReservationStatus(testFutureReservation);
        assert (queuedReservation.getStatus().equals(ReservationStatus.FUTURE));
    }

    /**
     * Test intializeReservationStatus method with current reservation.
     */
    @Test
    public void testIntializeTodayReservationStatus() {
        ReservationManager reservationManagerSpy = spy(reservationManager);
        Mockito.doReturn(testReservation).when(reservationManagerSpy).addEntityToReservation(Matchers.any());
        Reservation queuedReservation =
                reservationManagerSpy.intializeReservationStatus(testReservation);
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
        Mockito.doNothing().when(reservationManagerSpy).updateReservationResult(anySet());
        when(reservationDao.getAllReservations())
                .thenReturn(new HashSet<>(Arrays.asList(unfulfilledReservation)));
        FindInitialPlacementRequest findInitialPlacementRequest = FindInitialPlacementRequest.newBuilder()
                .addInitialPlacementBuyer(InitialPlacementBuyer.newBuilder().setBuyerId(6).build()).build();
        Mockito.when(reservationManagerSpy.buildIntialPlacementRequest(anySet())).thenReturn(findInitialPlacementRequest);
        reservationManagerSpy.checkAndStartReservationPlan();
        when(initialPlacementService.findInitialPlacement(findInitialPlacementRequest))
                .thenReturn(FindInitialPlacementResponse.getDefaultInstance());
        try {
            verify(reservationDao, times(1)).updateReservationBatch(updateBatchCaptor.capture());
            Set<Reservation> updatedReservations = updateBatchCaptor.getValue();
            assertThat(updatedReservations, containsInAnyOrder(unfulfilledReservation.toBuilder()
                    .setStatus(ReservationStatus.INPROGRESS)
                    .build()));
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }
        verify(initialPlacementService, times(1)).findInitialPlacement(findInitialPlacementRequest);
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
        verify(initialPlacementService, times(0)).findInitialPlacement(Matchers.any());
    }


    /**
     * End to end testing for fast reservation success scenario. Add the
     * fake entity to the reservation using addEntityToReservation. Build the
     * initial placement request using buildIntialPlacementRequest.
     * Create a "success like" response findInitialPlacementResponseBuilder.
     * Update the provider info from the response using updateProviderInfoForReservations.
     * Update reservation status using updateReservationResult after the providers are updated.
     * Verify the reservation status is RESERVED.
     */
    @Test
    public void testFastReservationSuccess() {
        IdentityGenerator.initPrefix(0);
        ReservationManager reservationManagerSpy = spy(reservationManager);
        when(templatesDao.getTemplate(234L)).thenReturn(java.util.Optional.ofNullable(template));
        Reservation updateReservation =
                reservationManagerSpy.addEntityToReservation(fastReservation);
        Set<Reservation> reservations = new HashSet<>();
        reservations.add(updateReservation);
        FindInitialPlacementRequest findInitialPlacementRequest =
                reservationManagerSpy.buildIntialPlacementRequest(reservations);
        FindInitialPlacementResponse.Builder findInitialPlacementResponseBuilder =
                FindInitialPlacementResponse.newBuilder();
        Reservation reservation = updateReservation;
        for (ReservationTemplate reservationTemplate : reservation.getReservationTemplateCollection().getReservationTemplateList()) {
            for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                    findInitialPlacementResponseBuilder.addInitialPlacementBuyerPlacementInfo(
                            InitialPlacementBuyerPlacementInfo.newBuilder()
                                    .setBuyerId(reservationInstance.getEntityId()).setCommoditiesBoughtFromProviderId(placementInfo.getPlacementInfoId()).setProviderId(100));
                }
            }
        }

        when(reservationDao.getAllReservations())
                .thenReturn(Sets.newHashSet(updateReservation));
        FindInitialPlacementResponse findInitialPlacementResponse =
                findInitialPlacementResponseBuilder.build();
        Set<Reservation> updatedReservations = reservationManagerSpy
                .updateProviderInfoForReservations(findInitialPlacementResponse);
        ArgumentCaptor<HashSet<Reservation>> captor =
                ArgumentCaptor.forClass((Class<HashSet<Reservation>>)(Class)HashSet
                        .class);
        reservationManagerSpy.updateReservationResult(updatedReservations);
        try {
            verify(reservationDao, times(1))
                    .updateReservationBatch(captor.capture());
            Set<Long> reservedReservations =
                    captor.getValue().stream()
                            .filter(a -> a.getStatus() == ReservationStatus.RESERVED)
                            .map(a -> a.getId())
                            .collect(Collectors.toSet());
            assert (reservedReservations.contains(10000L));
            // assert the provider is updated
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }

    }

    /**
     * End to end testing for fast reservation failure scenario. Add the
     * fake entity to the reservation using addEntityToReservation. Build the
     * initial placement request using buildIntialPlacementRequest.
     * Create a "failure like" response findInitialPlacementResponseBuilder.
     * Update the provider info from the response using updateProviderInfoForReservations.
     * Update reservation status using updateReservationResult after the providers are updated.
     * Verify the reservation status is PLACEMENT_FAILED.
     */
    @Test
    public void testFastReservationFailure() {
        IdentityGenerator.initPrefix(0);
        ReservationManager reservationManagerSpy = spy(reservationManager);
        when(templatesDao.getTemplate(234L)).thenReturn(java.util.Optional.ofNullable(template));
        Reservation updateReservation =
                reservationManagerSpy.addEntityToReservation(fastReservation);
        Set<Reservation> reservations = new HashSet<>();
        reservations.add(updateReservation);
        FindInitialPlacementRequest findInitialPlacementRequest =
                reservationManagerSpy.buildIntialPlacementRequest(reservations);
        FindInitialPlacementResponse.Builder findInitialPlacementResponseBuilder =
                FindInitialPlacementResponse.newBuilder();
        when(reservationDao.getAllReservations())
                .thenReturn(Sets.newHashSet(updateReservation));
        Long buyerid = updateReservation.getReservationTemplateCollection()
                .getReservationTemplate(0)
                .getReservationInstance(0).getEntityId();
        Reservation reservation = updateReservation;
        for (ReservationTemplate reservationTemplate : reservation.getReservationTemplateCollection().getReservationTemplateList()) {
            for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                if (reservationInstance.getEntityId() == buyerid) {
                    for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                        findInitialPlacementResponseBuilder.addInitialPlacementBuyerPlacementInfo(
                                InitialPlacementBuyerPlacementInfo.newBuilder()
                                        .setBuyerId(buyerid).setCommoditiesBoughtFromProviderId(placementInfo.getPlacementInfoId()).setProviderId(100));
                    }
                }
            }
        }
        FindInitialPlacementResponse findInitialPlacementResponse =
                findInitialPlacementResponseBuilder.build();
        Set<Reservation> updatedReservations = reservationManagerSpy
                .updateProviderInfoForReservations(findInitialPlacementResponse);
        ArgumentCaptor<HashSet<Reservation>> captor =
                ArgumentCaptor.forClass((Class<HashSet<Reservation>>)(Class)HashSet
                        .class);
        reservationManagerSpy.updateReservationResult(updatedReservations);
        try {
            verify(reservationDao, times(1))
                    .updateReservationBatch(captor.capture());
            Set<Long> failedReservations =
                    captor.getValue().stream()
                            .filter(a -> a.getStatus() == ReservationStatus.PLACEMENT_FAILED)
                            .map(a -> a.getId())
                            .collect(Collectors.toSet());
            assert (failedReservations.contains(10000L));
            // assert the provider is updated
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
        ReservationManager reservationManagerSpy = Mockito.spy(reservationManager);
        Reservation reservedWithNewProvider = Reservation.newBuilder()
                .setId(1010)
                .setName("test-reservation")
                .setStatus(ReservationStatus.RESERVED)
                .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(ReservationTemplate.newBuilder()
                                .setCount(1L)
                                .setTemplate(template)
                                .addReservationInstance(ReservationInstance.newBuilder()
                                        .addPlacementInfo(PlacementInfo.newBuilder()
                                                .setProviderId(600L)))))
                .build();

        HashSet<Reservation> inProgressReservation =
                new HashSet<>(Arrays.asList(inProgressReservation1,
                        inProgressReservation2,
                        inProgressReservation3,
                        reservedWithNewProvider));
        ArgumentCaptor<HashSet<Reservation>> captor =
                ArgumentCaptor.forClass((Class<HashSet<Reservation>>)(Class)HashSet
                        .class);
        reservationManagerSpy.updateReservationResult(inProgressReservation);
        try {
            verify(reservationDao, times(1))
                    .updateReservationBatch(captor.capture());
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

            // We should call the updateReservationBatch for all reservations including the ones
            // whose status didn't change..Because the provider could have changed.
            assert (reservedReservations.contains(1005L));
            assert (reservedReservations.contains(1010L));
            assert (failedReservations.contains(1003L));
            assert (failedReservations.contains(1004L));
            // assert the provider is updated
        } catch (NoSuchObjectException e) {
            e.printStackTrace();
        }


        verify(reservationManagerSpy, times(1))
                .broadcastReservationChange(captor.capture());
        Set<Long> statusUpdatedReservations =
                    captor.getValue().stream()
                            .map(a -> a.getId())
                            .collect(Collectors.toSet());
        // we should not broadcast the reservation change to the UI if the status does not change.
        // The provider change happens only during the main market and that need not be updated
        // in the UI. A refresh of screen will get the user the new providers.
        assert (statusUpdatedReservations.contains(1005L));
        assert (!statusUpdatedReservations.contains(1010L));
        assert (statusUpdatedReservations.contains(1003L));
        assert (statusUpdatedReservations.contains(1004L));

    }

    /**
     * Test that a reservation plan failure marks in-progress reservations as invalid.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPlanFailure() throws Exception {
        when(reservationDao.getAllReservations())
            .thenReturn(Sets.newHashSet(inProgressReservation1, inProgressReservation2, inProgressReservation3, unfulfilledReservation));

        reservationManager.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(1)
                .setProjectType(PlanProjectType.RESERVATION_PLAN)
                .setStatus(PlanStatus.FAILED)
                .build());

        verify(reservationDao).updateReservationBatch(updateBatchCaptor.capture());

        final Set<Reservation> updatedReservations = updateBatchCaptor.getValue();

        // We should have updated the in-progress reservation to invalid.
        for (Reservation reservation : updatedReservations) {
            assertTrue(reservation.getStatus() == ReservationStatus.INVALID);
            for (ReservationTemplate reservationTemplate
                    : reservation.getReservationTemplateCollection().getReservationTemplateList()) {
                for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                    for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                        assertFalse(placementInfo.hasProviderId());
                    }
                }
            }

        }
    }

    /**
     * Test that broadcasting a reservation change will appropriately invoke the message sender.
     *
     * @throws Exception if an error occurs.
     */
    @Test
    public void testbroadcastReservationChange() throws Exception {
        final Set<ReservationDTO.Reservation> newReservations = new HashSet<>();
        newReservations.add(testReservationForBroadcast1);
        newReservations.add(testReservationForBroadcast2);

        ArgumentCaptor<ReservationChanges> resChangesCaptor =
                ArgumentCaptor.forClass(ReservationChanges.class);
        reservationManager.broadcastReservationChange(newReservations);

        verify(sender).sendMessage(resChangesCaptor.capture());
        final ReservationChanges resChanges = resChangesCaptor.getValue();

        assertEquals(2, resChanges.getReservationChangeCount());

        Set<Long> reservedReservations =
                resChanges.getReservationChangeList().stream()
                        .filter(a -> ReservationStatus.RESERVED == a.getStatus())
                        .map(a -> a.getId())
                        .collect(Collectors.toSet());
        Set<Long> failedReservations =
                resChanges.getReservationChangeList().stream()
                        .filter(a -> a.getStatus() == ReservationStatus.PLACEMENT_FAILED)
                        .map(a -> a.getId())
                        .collect(Collectors.toSet());
        assert (reservedReservations.size() == 1);
        assert (reservedReservations.contains(1008L));
        assert (failedReservations.size() == 1);
        assert (failedReservations.contains(1009L));
    }

}
