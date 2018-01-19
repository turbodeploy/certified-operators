package com.vmturbo.plan.orchestrator.reservation;

import org.assertj.core.util.Lists;
import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;

public class ReservationPlacementHandlerTest {

    private RepositoryServiceMole repositoryServiceMole = Mockito.spy(new RepositoryServiceMole());

    private final ReservationDao reservationDao = Mockito.mock(ReservationDao.class);

    private ReservationPlacementHandler reservationPlacementHandler;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(repositoryServiceMole);

    @Before
    public void setup() {
        reservationPlacementHandler = new ReservationPlacementHandler(reservationDao,
                RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    @Test
    public void testUpdateReservations() throws Exception {
        final long contextId = 7777;
        final long topologyId = 123456;
        final Reservation reservation = Reservation.newBuilder()
                .setName("Test-reservation")
                .setId(123)
                .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(ReservationTemplate.newBuilder()
                                .setTemplateId(456)
                                .setCount(1)
                                .addReservationInstance(ReservationInstance.newBuilder()
                                        .setEntityId(1)
                                        .addPlacementInfo(PlacementInfo.newBuilder()
                                                .setProviderId(2)
                                                .setProviderType(14)))))
                .build();
        final Reservation newReservation = Reservation.newBuilder()
                .setName("Test-reservation")
                .setId(123)
                .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                        .addReservationTemplate(ReservationTemplate.newBuilder()
                                .setTemplateId(456)
                                .setCount(1)
                                .addReservationInstance(ReservationInstance.newBuilder()
                                        .setEntityId(1)
                                        .addPlacementInfo(PlacementInfo.newBuilder()
                                                .setProviderId(3)
                                                .setProviderType(14)))))
                .build();
        final TopologyEntityDTO reservationEntity = TopologyEntityDTO.newBuilder()
                .setOid(1)
                .setEntityType(10)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(3)
                        .setProviderEntityType(14))
                .build();
        final TopologyEntityDTO providerEntity = TopologyEntityDTO.newBuilder()
                .setOid(3)
                .setEntityType(14)
                .build();
        Mockito.when(reservationDao.getReservationsByStatus(ReservationStatus.RESERVED))
                .thenReturn(ImmutableSet.of(reservation));
        final RetrieveTopologyEntitiesRequest entityRequest = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId)
                .setTopologyType(TopologyType.PROJECTED)
                .addAllEntityOids(Lists.newArrayList(1L))
                .build();
        final RetrieveTopologyEntitiesRequest providerRequest = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(contextId)
                .setTopologyId(topologyId)
                .setTopologyType(TopologyType.PROJECTED)
                .addAllEntityOids(Lists.newArrayList(3L))
                .build();
        Mockito.when(repositoryServiceMole.retrieveTopologyEntities(entityRequest))
                .thenReturn(RetrieveTopologyEntitiesResponse.newBuilder()
                        .addEntities(reservationEntity)
                        .build());
        Mockito.when(repositoryServiceMole.retrieveTopologyEntities(providerRequest))
                .thenReturn(RetrieveTopologyEntitiesResponse.newBuilder()
                        .addEntities(providerEntity)
                        .build());
        reservationPlacementHandler.updateReservations(contextId, topologyId);
        Mockito.verify(reservationDao, Mockito.times(1))
                .updateReservationBatch(ImmutableSet.of(newReservation));
    }
}