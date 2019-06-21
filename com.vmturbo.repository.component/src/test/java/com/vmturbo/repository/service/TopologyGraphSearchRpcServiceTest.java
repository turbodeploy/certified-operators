package com.vmturbo.repository.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagsResponse;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.repository.listener.realtime.GlobalSupplyChainCalculator;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.search.SearchResolver;

/**
 *
 */
public class TopologyGraphSearchRpcServiceTest {

    private LiveTopologyStore liveTopologyStore = new LiveTopologyStore(GlobalSupplyChainCalculator.newFactory().newCalculator());
    private SearchResolver searchResolver = Mockito.mock(SearchResolver.class);
    private LiveTopologyPaginator liveTopologyPaginator = Mockito.mock(LiveTopologyPaginator.class);

    private PartialEntityConverter partialEntityConverter = new PartialEntityConverter();
    private UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);


    TopologyGraphSearchRpcService topologyGraphSearchRpcService = new TopologyGraphSearchRpcService(liveTopologyStore,
            searchResolver, liveTopologyPaginator, partialEntityConverter, userSessionContext, 1);

    @Test
    public void testInternalSearchWithScopedUser() {
        SourceRealtimeTopologyBuilder topologyBuilder = liveTopologyStore.newRealtimeTopology(TopologyInfo.getDefaultInstance());
        topologyBuilder.addEntities(createTestEntities(1L, 2L, 3L));
        topologyBuilder.finish();

        Collection<Long> resultOids = topologyGraphSearchRpcService.internalSearch(Arrays.asList(1L,2L), Collections.emptyList())
                .map(RepoGraphEntity::getOid)
                .collect(Collectors.toSet());
        // verify the unscoped user can see all entities.
        Assert.assertThat(resultOids, containsInAnyOrder(1L,2L));

        // now set up a scoped user and try again
        List<Long> accessibleEntities = Arrays.asList(2L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        Mockito.when(userSessionContext.isUserScoped()).thenReturn(true);
        Mockito.when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        Collection<Long> scopedResultOids = topologyGraphSearchRpcService.internalSearch(Arrays.asList(1L,2L), Collections.emptyList())
                .map(RepoGraphEntity::getOid)
                .collect(Collectors.toSet());
        // verify the scoped user can only see entity 2
        Assert.assertThat(scopedResultOids, containsInAnyOrder(2L));
        Assert.assertThat(scopedResultOids, not(containsInAnyOrder(1L)));
    }

    @Test
    public void testSearchTagsWithScopedUser() {
        SourceRealtimeTopologyBuilder topologyBuilder = liveTopologyStore.newRealtimeTopology(TopologyInfo.getDefaultInstance());
        topologyBuilder.addEntities(createTestEntities(1L, 2L, 3L));
        topologyBuilder.finish();

        SearchTagsRequest request = SearchTagsRequest.newBuilder()
                .addAllEntities(Arrays.asList(1L, 2L, 3L))
                .build();
        StreamObserver<SearchTagsResponse> responseObserver = Mockito.mock(StreamObserver.class);

        topologyGraphSearchRpcService.searchTags(request, responseObserver);

        ArgumentCaptor<SearchTagsResponse> responseCaptor = ArgumentCaptor.forClass(SearchTagsResponse.class);
        Mockito.verify(responseObserver).onNext(responseCaptor.capture());

        // verify that the non-scoped user can see all tags.
        SearchTagsResponse response = responseCaptor.getValue();
        Map<String, TagValuesDTO> tags = response.getTags().getTagsMap();
        Assert.assertTrue(tags.containsKey("1"));
        Assert.assertTrue(tags.containsKey("2"));
        Assert.assertTrue(tags.containsKey("3"));

        // now set up a scoped user and try again
        List<Long> accessibleEntities = Arrays.asList(2L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        Mockito.when(userSessionContext.isUserScoped()).thenReturn(true);
        Mockito.when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        topologyGraphSearchRpcService.searchTags(request, responseObserver);
        Mockito.verify(responseObserver, Mockito.times(2)).onNext(responseCaptor.capture());

        // verify that the non-scoped user can only see tag #2.
        tags = responseCaptor.getValue().getTags().getTagsMap();
        Assert.assertFalse(tags.containsKey("1"));
        Assert.assertTrue(tags.containsKey("2"));
        Assert.assertFalse(tags.containsKey("3"));
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
                .setTags(Tags.newBuilder()
                        .putTags(String.valueOf(oid), TagValuesDTO.newBuilder()
                                .addValues(String.valueOf(oid))
                                .build()))
                .build();
    }


}
