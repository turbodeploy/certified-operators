package com.vmturbo.repository.service;

import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * Tests class {@link TopologyGraphRepositoryRpcService}.
 */
public class TopologyGraphRepositoryRpcServiceTest {

    private LiveTopologyStore liveTopologyStore = new LiveTopologyStore(new GlobalSupplyChainCalculator());

    private final ArangoRepositoryRpcService arangoRepoRpcService = Mockito.mock(ArangoRepositoryRpcService.class);

    private UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);

    private long REALTIME_CONTEXT_ID = 777777L;

    private PartialEntityConverter partialEntityConverter = new PartialEntityConverter();

    private TopologyGraphRepositoryRpcService repositoryRpcService = new TopologyGraphRepositoryRpcService(
        liveTopologyStore, arangoRepoRpcService, partialEntityConverter,
        REALTIME_CONTEXT_ID, 1000, userSessionContext);

    @Test
    public void testRetrieveTopologyEntitiesWithScopedUser() {
        // test the retrieveTopologyEntities() method when called by a scoped user.

        SourceRealtimeTopologyBuilder topologyBuilder = liveTopologyStore.newRealtimeSourceTopology(TopologyInfo.getDefaultInstance());
        topologyBuilder.addEntities(createTestEntities(1L, 2L, 3L));
        topologyBuilder.finish();

        RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(REALTIME_CONTEXT_ID)
                .addAllEntityOids(Arrays.asList(1L,2L,3L))
                .build();

        StreamObserver<PartialEntityBatch> responseObserver = Mockito.mock(StreamObserver.class);
        ArgumentCaptor<PartialEntityBatch> responseCaptor = ArgumentCaptor.forClass(PartialEntityBatch.class);

        repositoryRpcService.retrieveTopologyEntities(request, responseObserver);

        Mockito.verify(responseObserver).onNext(responseCaptor.capture());

        Set<Long> responseOids = responseCaptor.getValue().getEntitiesList().stream()
            .map(TopologyDTOUtil::getOid)
            .collect(Collectors.toSet());
        Assert.assertThat(responseOids, containsInAnyOrder(1L,2L, 3L));


        // now set up a scoped user and try again
        List<Long> accessibleEntities = Arrays.asList(2L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        Mockito.when(userSessionContext.isUserScoped()).thenReturn(true);
        Mockito.when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        repositoryRpcService.retrieveTopologyEntities(request, responseObserver);

        Mockito.verify(responseObserver, Mockito.times(2)).onNext(responseCaptor.capture());

        // verify that the non-scoped user can only see entity 2.
        responseOids = responseCaptor.getValue().getEntitiesList().stream()
                .map(TopologyDTOUtil::getOid)
                .collect(Collectors.toSet());
        Assert.assertFalse(responseOids.contains(1L));
        Assert.assertTrue(responseOids.contains(2L));
        Assert.assertFalse(responseOids.contains(3L));
    }

    private Collection<TopologyEntityDTO> createTestEntities(long... oids) {
        Collection<TopologyEntityDTO> entities = new ArrayList<>(oids.length);
        for (long oid : oids) {
            entities.add(createTestEntity(oid));
        }
        return entities;
    }

    private TopologyEntityDTO createTestEntity(long oid) {
        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(1)
                .build();
    }
}
