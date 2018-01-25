package com.vmturbo.topology.processor.reservation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation.Date;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

public class ReservationManagerTest {

    private ReservationServiceMole reservationServiceMole = Mockito.spy(new ReservationServiceMole());

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

    final TopologyEntityDTO.Builder topologyEntityBuild = TopologyEntityDTO.newBuilder()
            .setOid(345)
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
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationServiceMole);

    @Before
    public void setup() {
        reservationManager = new ReservationManager(
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                templateConverterFactory);
    }

    @Test
    public void testApplyReservationReserved() {
        Mockito.when(reservationServiceMole.getAllReservations(
                GetAllReservationsRequest.newBuilder().build()))
                .thenReturn(Lists.newArrayList(reservation));
        Mockito.when(templateConverterFactory.generateTopologyEntityFromTemplates(
                Mockito.anyMap(), Mockito.any()))
                .thenReturn(Lists.newArrayList(topologyEntityBuild).stream());
        final Map<Long, Builder> topology = new HashMap<>();
        reservationManager.applyReservation(topology);
        System.out.println(topology);
        assertEquals(1L, topology.size());
        assertTrue(topology.containsKey(1L));
        final TopologyEntityDTO.Builder builder = topology.get(1L).getEntityBuilder();
        assertEquals(2L, builder.getCommoditiesBoughtFromProvidersCount());
        assertTrue(builder.getCommoditiesBoughtFromProvidersBuilderList().stream()
            .allMatch(commoditiesBought -> !commoditiesBought.hasProviderId()));
    }

    @Test
    public void testApplyReservationFuture() {
        final LocalDate today = LocalDate.now(DateTimeZone.UTC);
        final LocalDate nextMonth = LocalDate.now(DateTimeZone.UTC).plusMonths(1);
        final Reservation futureReservation = Reservation.newBuilder(reservation)
                .setStatus(ReservationStatus.FUTURE)
                .setStartDate(Date.newBuilder()
                        .setYear(today.getYear())
                        .setMonth(today.getMonthOfYear())
                        .setDay(today.getDayOfMonth()))
                .setExpirationDate(Date.newBuilder()
                        .setYear(nextMonth.getYear())
                        .setMonth(nextMonth.getMonthOfYear())
                        .setDay(nextMonth.getDayOfMonth()))
                .build();
        Mockito.when(reservationServiceMole.getAllReservations(
                GetAllReservationsRequest.newBuilder().build()))
                .thenReturn(Lists.newArrayList(futureReservation));
        Mockito.when(templateConverterFactory.generateTopologyEntityFromTemplates(
                Mockito.anyMap(), Mockito.any()))
                .thenReturn(Lists.newArrayList(topologyEntityBuild).stream());
        final Map<Long, Builder> topology = new HashMap<>();

        // test reservation should be active now
        final boolean isActiveNow = reservationManager.isReservationActiveNow(futureReservation);
        assertTrue(isActiveNow);
        final List<Reservation> updateReservations =
                reservationManager.handlePotentialActiveReservation(Sets.newHashSet(futureReservation),
                        new ArrayList<>());
        assertFalse(updateReservations.isEmpty());
        Mockito.when(templateConverterFactory.generateTopologyEntityFromTemplates(
                Mockito.anyMap(), Mockito.any()))
                .thenReturn(Lists.newArrayList(topologyEntityBuild).stream());
        reservationManager.applyReservation(topology);
        assertEquals(1L, topology.size());
        assertTrue(topology.containsKey(345L));
        final TopologyEntityDTO.Builder builder = topology.get(345L).getEntityBuilder();
        assertEquals(2L, builder.getCommoditiesBoughtFromProvidersCount());
        assertTrue(builder.getCommoditiesBoughtFromProvidersBuilderList().stream()
                .allMatch(commoditiesBought -> !commoditiesBought.hasProviderId()));
        Mockito.verify(reservationServiceMole, Mockito.times(1))
                .updateReservations(Mockito.any(), Mockito.any());
    }
}
