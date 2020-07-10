package com.vmturbo.topology.processor.reservation;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.reservation.ReservationValidator.ValidationErrors;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

public class ReservationManagerTest {
    private double epsilon = 1e-5;

    private ReservationServiceMole reservationService = Mockito.spy(ReservationServiceMole.class);

    private TemplateConverterFactory templateConverterFactory =
            mock(TemplateConverterFactory.class);

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
                                            .setProviderType(PHYSICAL_MACHINE_VALUE)
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
                    .setProviderEntityType(PHYSICAL_MACHINE_VALUE)
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
                    .setProviderEntityType(PHYSICAL_MACHINE_VALUE)
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
            .setEntityType(PHYSICAL_MACHINE_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setUsed(10)
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE)));

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservationService);

    private ReservationValidator validator = mock(ReservationValidator.class);

    @Before
    public void setup() {
        reservationManager = new ReservationManager(
            ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            validator);
    }

    /**
     * Verify that invalid reservations get marked as INVALID in the reservation service.
     */
    @Test
    public void testMarkInvalidReservations() {
        doReturn(Collections.singletonList(reservation))
            .when(reservationService).getAllReservations(any());

        final ValidationErrors errors = mock(ValidationErrors.class);
        when(errors.getErrorsByReservation())
            .thenReturn(Collections.singletonMap(reservation.getId(), Collections.singletonList("BAD")));
        when(errors.isEmpty()).thenReturn(false);

        when(validator.validateReservations(any(), any())).thenReturn(errors);
        TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME).build();
        reservationManager.applyReservation(Collections.emptyMap(),
                topologyInfo);

        final ArgumentCaptor<UpdateReservationsRequest> updateCaptor =
            ArgumentCaptor.forClass(UpdateReservationsRequest.class);
        verify(reservationService).updateReservations(updateCaptor.capture());

        for (Reservation reservation : updateCaptor.getValue().getReservationList()) {
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
     * Test {@link ReservationManager#applyReservation(Map, TopologyInfo)} with
     * one reservation.
     * @throws Exception any test error
     */
    @Test
    public void testApplyReservationReserved() throws Exception {
        doReturn(Collections.singletonList(reservation))
            .when(reservationService).getAllReservations(any());
        when(validator.validateReservations(any(), any())).thenReturn(new ValidationErrors());

        final Map<Long, Builder> topology = new HashMap<>();
        topology.put(providerEntity.getOid(), TopologyEntity.newBuilder(providerEntity));
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build();
        reservationManager.applyReservation(topology, topologyInfo);

        assertEquals(2L, topology.size());
        assertTrue(topology.containsKey(1L));
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
        assertEquals(10.0, commoditySoldDTOs.get(0).getUsed(), epsilon);
    }


    @Ignore
    @Test
    public void testApplyReservationReservedAndFuture() throws Exception {
        final Map<Long, Builder> topology = new HashMap<>();
        ArgumentCaptor<UpdateReservationsRequest> updateRequestCaptor =
                ArgumentCaptor.forClass(UpdateReservationsRequest.class);
        topology.put(providerEntity.getOid(), TopologyEntity.newBuilder(providerEntity));
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME)
                .setPlanInfo(PlanTopologyInfo.newBuilder().setPlanProjectType(PlanProjectType.USER)).build();
        reservationManager.applyReservation(topology, topologyInfo);
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
        verify(reservationService, Mockito.times(1))
                .updateReservations(updateRequestCaptor.capture(), any());
        final UpdateReservationsRequest updateReservationsRequest = updateRequestCaptor.getValue();
        assertEquals(1, updateReservationsRequest.getReservationCount());
    }
}
