package com.vmturbo.topology.processor.reservation;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.ReservationDTO.CreateReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation.Date;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

public class ReservationManagerTest {

    private TestReservationService reservationService = Mockito.spy(new TestReservationService());

    private TemplateConverterFactory templateConverterFactory =
            Mockito.mock(TemplateConverterFactory.class);

    private ReservationManager reservationManager;

    final Reservation reservation = Reservation.newBuilder()
            .setName("Test-reservation")
            .setId(123)
            .setStatus(ReservationStatus.RESERVED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setTemplateId(456)
                            .setCount(1)
                            .addReservationInstance(ReservationInstance.newBuilder()
                                    .setEntityId(1)
                                    .setName("Test-reservation_0")
                                    .addPlacementInfo(PlacementInfo.newBuilder()
                                            .setProviderId(222)
                                            .setProviderType(14))
                                    .addPlacementInfo(PlacementInfo.newBuilder()
                                            .setProviderId(333)
                                            .setProviderType(2)))))
            .build();

    final LocalDate today = LocalDate.now(DateTimeZone.UTC);
    final LocalDate nextMonth = LocalDate.now(DateTimeZone.UTC).plusMonths(1);
    final Reservation futureReservation = Reservation.newBuilder(reservation)
            .setId(234)
            .setStatus(ReservationStatus.FUTURE)
            .setStartDate(Date.newBuilder()
                    .setYear(today.getYear())
                    .setMonth(today.getMonthOfYear())
                    .setDay(today.getDayOfMonth()))
            .setExpirationDate(Date.newBuilder()
                    .setYear(nextMonth.getYear())
                    .setMonth(nextMonth.getMonthOfYear())
                    .setDay(nextMonth.getDayOfMonth()))
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setTemplateId(567)
                            .setCount(1)))
            .build();

    final TopologyEntityDTO.Builder topologyEntityBuildReserved = TopologyEntityDTO.newBuilder()
            .setOid(111)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(14)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setKey("test-commodity-key")
                                    .setType(56))))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(2)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setKey("test-commodity-key-two")
                                    .setType(8))));

    final TopologyEntityDTO.Builder topologyEntityBuildFuture = TopologyEntityDTO.newBuilder()
            .setOid(2)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(14)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setKey("test-commodity-key")
                                    .setType(56))))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(2)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setKey("test-commodity-key-two")
                                    .setType(8))));

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationService);

    @Before
    public void setup() {
        reservationManager = new ReservationManager(
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                templateConverterFactory);
    }

    @Test
    public void testApplyReservationReservedAndFuture() {
        Mockito.when(templateConverterFactory.generateTopologyEntityFromTemplates(
                (Map<Long, Long>) argThat(hasEntry(456L, 1L)), Mockito.any()))
                .thenReturn(Lists.newArrayList(topologyEntityBuildReserved).stream());
        Mockito.when(templateConverterFactory.generateTopologyEntityFromTemplates(
                (Map<Long, Long>) argThat(hasEntry(567L, 1L)), Mockito.any()))
                .thenReturn(Lists.newArrayList(topologyEntityBuildFuture).stream());
        final Map<Long, Builder> topology = new HashMap<>();
        // test future reservation should be active now
        final boolean isActiveNow = reservationManager.isReservationActiveNow(futureReservation);
        assertTrue(isActiveNow);

        reservationManager.applyReservation(topology);
        assertEquals(2L, topology.size());
        assertTrue(topology.containsKey(1L));
        assertTrue(topology.containsKey(2L));

        // Check already reserved Reservation
        final TopologyEntityDTO.Builder builderReserved = topology.get(1L).getEntityBuilder();
        assertEquals(2L, builderReserved.getCommoditiesBoughtFromProvidersCount());
        assertTrue(builderReserved.getCommoditiesBoughtFromProvidersBuilderList().stream()
                .allMatch(commoditiesBought -> !commoditiesBought.hasProviderId()));

        // Check just become active Reservation
        final TopologyEntityDTO.Builder builderFuture = topology.get(2L).getEntityBuilder();
        assertEquals(2L, builderFuture.getCommoditiesBoughtFromProvidersCount());
        assertTrue(builderFuture.getCommoditiesBoughtFromProvidersBuilderList().stream()
                .allMatch(commoditiesBought -> !commoditiesBought.hasProviderId()));
        Mockito.verify(reservationService, Mockito.times(1))
                .updateReservations(Mockito.any(), Mockito.any());
    }

    private class TestReservationService extends ReservationServiceImplBase {

        @Override
        public void initialPlacement(InitialPlacementRequest request,
                                     StreamObserver<InitialPlacementResponse> responseObserver) {
            responseObserver.onNext(InitialPlacementResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void getAllReservations(GetAllReservationsRequest request,
                                       StreamObserver<Reservation> responseObserver) {
            responseObserver.onNext(reservation);
            responseObserver.onNext(futureReservation);
            responseObserver.onCompleted();
        }

        @Override
        public void getReservationById(GetReservationByIdRequest request,
                                       StreamObserver<Reservation> responseObserver) {
            responseObserver.onNext(Reservation.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void getReservationByStatus(GetReservationByStatusRequest request,
                                           StreamObserver<Reservation> responseObserver) {
            responseObserver.onNext(Reservation.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void createReservation(CreateReservationRequest request,
                                      StreamObserver<Reservation> responseObserver) {
            responseObserver.onNext(Reservation.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteReservationById(DeleteReservationByIdRequest request,
                                          StreamObserver<Reservation> responseObserver) {
            responseObserver.onNext(Reservation.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void updateReservationById(UpdateReservationByIdRequest request,
                                          StreamObserver<Reservation> responseObserver) {
            responseObserver.onNext(Reservation.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void updateReservations(UpdateReservationsRequest request,
                                       StreamObserver<Reservation> responseObserver) {
            responseObserver.onNext(Reservation.getDefaultInstance());
            responseObserver.onCompleted();
        }

    }
}
