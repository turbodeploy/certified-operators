package com.vmturbo.topology.processor.reservation;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.argThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
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
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

public class ReservationManagerTest {
    private double epsilon = 1e-5;

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
                                            .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)
                                            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                                    .setCommodityType(CommodityType.newBuilder()
                                                            .setType((CommodityDTO
                                                                    .CommodityType.MEM_PROVISIONED_VALUE)))
                                                    .setUsed(100))
                                            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                                    .setUsed(200)
                                                    .setCommodityType(CommodityType.newBuilder()
                                                            .setType(CommodityDTO.CommodityType.MEM_VALUE))))
                                    .addPlacementInfo(PlacementInfo.newBuilder()
                                            .setProviderId(333)
                                            .setProviderType(EntityType.STORAGE_VALUE)
                                            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                                    .setCommodityType(CommodityType.newBuilder()
                                                            .setType((CommodityDTO
                                                                    .CommodityType.STORAGE_VALUE)))
                                                    .setUsed(100))))))
            .build();

    final Date today = new Date();
    LocalDateTime ldt = today.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
    final Date nextMonth = Date.from(ldt.plusMonths(1)
                    .atOffset(ZoneOffset.UTC).toInstant());
    final Reservation futureReservation = Reservation.newBuilder(reservation)
            .setId(234)
            .setStatus(ReservationStatus.FUTURE)
            .setStartDate(today.getTime())
            .setExpirationDate(nextMonth.getTime())
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                            .setTemplateId(567)
                            .setCount(1)))
            .build();

    final TopologyEntityDTO.Builder topologyEntityBuildReserved = TopologyEntityDTO.newBuilder()
            .setOid(111)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setType(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE)))
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(0.0)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setType(CommodityDTO.CommodityType.MEM_VALUE))))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.STORAGE_VALUE)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))));

    final TopologyEntityDTO.Builder topologyEntityBuildFuture = TopologyEntityDTO.newBuilder()
            .setOid(2)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setKey("test-commodity-key")
                                    .setType(56))))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderEntityType(EntityType.STORAGE_VALUE)
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setUsed(100)
                            .setCommodityType(CommodityType.newBuilder()
                                    .setKey("test-commodity-key-two")
                                    .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))));

    final TopologyEntityDTO.Builder providerEntity = TopologyEntityDTO.newBuilder()
            .setOid(222)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setUsed(10)
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE)));

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationService);

    @Before
    public void setup() {
        reservationManager = new ReservationManager(
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                templateConverterFactory);
    }

    @Ignore
    @Test
    public void testApplyReservationReservedAndFuture() {
        Mockito.when(templateConverterFactory.generateReservationEntityFromTemplates(
                (Map<Long, Long>) argThat(hasEntry(456L, 1L)), anyMap()))
                .thenReturn(Lists.newArrayList(topologyEntityBuildReserved).stream());
        Mockito.when(templateConverterFactory.generateReservationEntityFromTemplates(
                (Map<Long, Long>) argThat(hasEntry(567L, 1L)), anyMap()))
                .thenReturn(Lists.newArrayList(topologyEntityBuildFuture).stream());
        final Map<Long, Builder> topology = new HashMap<>();
        ArgumentCaptor<UpdateReservationsRequest> updateRequestCaptor =
                ArgumentCaptor.forClass(UpdateReservationsRequest.class);
        topology.put(providerEntity.getOid(), TopologyEntity.newBuilder(providerEntity));

        reservationManager.applyReservation(topology, TopologyType.REALTIME, PlanProjectType.USER);
        assertEquals(3L, topology.size());
        assertTrue(topology.containsKey(1L));
        assertTrue(topology.containsKey(2L));
        assertTrue(topology.containsKey(providerEntity.getOid()));

        // Check already reserved Reservation
        final TopologyEntityDTO.Builder builderReserved = topology.get(1L).getEntityBuilder();
        assertFalse(builderReserved.getAnalysisSettings().getSuspendable());
        assertEquals(2L, builderReserved.getCommoditiesBoughtFromProvidersCount());
        assertTrue(builderReserved.getCommoditiesBoughtFromProvidersBuilderList().stream()
                .anyMatch(commoditiesBought -> commoditiesBought.getProviderId() == providerEntity.getOid()));

        // Check provider commodity sold utilization changed correctly.
        final List<CommoditySoldDTO> commoditySoldDTOs = topology.get(providerEntity.getOid())
                .getEntityBuilder().getCommoditySoldListList();
        assertEquals(1, commoditySoldDTOs.size());
        assertEquals(110.0, commoditySoldDTOs.get(0).getUsed(), epsilon);

        // Check just become active Reservation
        final TopologyEntityDTO.Builder builderFuture = topology.get(2L).getEntityBuilder();
        assertFalse(builderFuture.getAnalysisSettings().getSuspendable());
        assertEquals(2L, builderFuture.getCommoditiesBoughtFromProvidersCount());
        assertTrue(builderFuture.getCommoditiesBoughtFromProvidersBuilderList().stream()
                .allMatch(commoditiesBought -> !commoditiesBought.hasProviderId()));
        Mockito.verify(reservationService, Mockito.times(1))
                .updateReservations(updateRequestCaptor.capture(), Mockito.any());
        final UpdateReservationsRequest updateReservationsRequest = updateRequestCaptor.getValue();
        assertEquals(1, updateReservationsRequest.getReservationCount());
    }

    @Test
    public void testModifyCommodityBought() {
        final TopologyEntityDTO.Builder providerEntity = TopologyEntityDTO.newBuilder()
                .setOid(123)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setUsed(100.0)
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE)));
        final TopologyEntity.Builder providerBuilder = TopologyEntity.newBuilder(providerEntity);
        final CommoditiesBoughtFromProvider.Builder commodityBoughtBuilder =
                CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE))
                                .setUsed(200.0));
        reservationManager.modifyProviderEntityCommodityBought(providerBuilder,
                commodityBoughtBuilder);
        assertEquals(300.0, providerBuilder.getEntityBuilder().getCommoditySoldList(0)
                .getUsed(), 0.00001);
    }

    private class TestReservationService extends ReservationServiceImplBase {


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
