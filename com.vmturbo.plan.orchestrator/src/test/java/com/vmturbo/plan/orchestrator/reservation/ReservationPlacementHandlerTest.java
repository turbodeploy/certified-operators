package com.vmturbo.plan.orchestrator.reservation;

import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ReservationPlacementHandlerTest {

    private RepositoryServiceMole repositoryServiceMole = Mockito.spy(new RepositoryServiceMole());

    private final ReservationManager reservationManager = Mockito.mock(ReservationManager.class);

    private final ReservationDao reservationDao = Mockito.mock(ReservationDao.class);

    private ReservationPlacementHandler reservationPlacementHandler;

    private final long contextId = 7777;

    private final long topologyId = 123456;

    private final CommodityBoughtDTO memCommodityBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.MEM_VALUE))
            .setUsed(100)
            .build();

    private final CommodityBoughtDTO cpuCommodityBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CPU_VALUE))
            .setUsed(3)
            .build();

    private final CommodityBoughtDTO storageCommodityBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.STORAGE_VALUE))
            .setUsed(200)
            .build();

    private final Reservation reservation = Reservation.newBuilder()
            .setName("Test-reservation")
            .setId(123)
            .setStatus(ReservationStatus.RESERVED)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                                    .setTemplateId(456)
                                    .setCount(1)
                                    .addReservationInstance(ReservationInstance.newBuilder()
                                            .setEntityId(1)
                                            .addPlacementInfo(PlacementInfo.newBuilder()
                                                    .setProviderId(2)
                                                    .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)))))
            .build();

    private final Reservation newReservation = Reservation.newBuilder()
            .setName("Test-reservation")
            .setStatus(ReservationStatus.RESERVED)
            .setId(123)
            .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                    .addReservationTemplate(ReservationTemplate.newBuilder()
                                    .setTemplateId(456)
                                    .setCount(1)
                                    .addReservationInstance(ReservationInstance.newBuilder()
                                            .setEntityId(1)
                                            .addPlacementInfo(PlacementInfo.newBuilder()
                                                    .setProviderId(3)
                                                    .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)
                                                    .addCommodityBought(memCommodityBought)
                                                    .addCommodityBought(cpuCommodityBought))
                                            .addPlacementInfo(PlacementInfo.newBuilder()
                                                    .addCommodityBought(storageCommodityBought)))))
            .build();

    private final TopologyEntityDTO reservationEntity = TopologyEntityDTO.newBuilder()
            .setOid(1)
            .setEntityType(10)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(3)
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .addCommodityBought(memCommodityBought)
                    .addCommodityBought(cpuCommodityBought))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .addCommodityBought(storageCommodityBought))
            .build();

    private final TopologyEntityDTO providerEntity = TopologyEntityDTO.newBuilder()
            .setOid(3)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(repositoryServiceMole);

    @Before
    public void setup() {
        reservationPlacementHandler = new ReservationPlacementHandler(reservationManager,
                RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    @Test
    public void testUpdateReservations() throws Exception {
        Mockito.when(reservationManager.getReservationDao())
                .thenReturn(reservationDao);
        Mockito.when(reservationManager.getReservationDao().getAllReservations())
                .thenReturn(ImmutableSet.of(reservation));
        final RetrieveTopologyEntitiesRequest entityRequest = RetrieveTopologyEntitiesRequest.newBuilder()
            .setTopologyContextId(contextId)
            .setTopologyId(topologyId)
            .setTopologyType(TopologyType.PROJECTED)
            .setReturnType(Type.FULL)
            .addAllEntityOids(Collections.singletonList(1L))
            .build();
        final RetrieveTopologyEntitiesRequest providerRequest = RetrieveTopologyEntitiesRequest.newBuilder()
            .setTopologyContextId(contextId)
            .setTopologyId(topologyId)
            .setTopologyType(TopologyType.PROJECTED)
            .setReturnType(Type.FULL)
            .addAllEntityOids(Collections.singletonList(3L))
            .build();
        Mockito.when(repositoryServiceMole.retrieveTopologyEntities(entityRequest))
                .thenReturn(ImmutableList.of(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setFullEntity(reservationEntity))
                        .build()));
        Mockito.when(repositoryServiceMole.retrieveTopologyEntities(providerRequest))
                .thenReturn(ImmutableList.of(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setFullEntity(providerEntity))
                        .build()));
        reservationPlacementHandler.updateReservationsFromLiveTopology(contextId, topologyId);
        Mockito.verify(reservationManager, Mockito.times(1))
                .updateReservationResult(ImmutableSet.of(newReservation));
    }

    @Test
    public void testUpdateNoReservations() {
        // if there are no reservations, there should be no requests for entities to the repository
        Mockito.when(reservationManager.getReservationDao())
                .thenReturn(reservationDao);
        Mockito.when(reservationManager.getReservationDao().getReservationsByStatus(ReservationStatus.RESERVED))
                .thenReturn(Collections.emptySet());
        reservationPlacementHandler.updateReservationsFromLiveTopology(contextId, topologyId);
        Mockito.verifyZeroInteractions(repositoryServiceMole);
    }
}