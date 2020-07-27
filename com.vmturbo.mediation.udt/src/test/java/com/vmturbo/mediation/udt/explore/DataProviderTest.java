package com.vmturbo.mediation.udt.explore;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link DataProvider}.
 */
public class DataProviderTest {

    SearchFilterResolver resolver = Mockito.mock(SearchFilterResolver.class);

    /**
     * Tests that 'getEntitiesByTag' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testGetEntitiesByTag() {
        String tag = "Region";
        EntityType type = EntityType.VIRTUAL_MACHINE;
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);
        SearchEntitiesRequest request = SearchEntitiesRequest.newBuilder().build();
        Mockito.when(requests.entitiesByTagRequest(tag, type)).thenReturn(request);
        SearchEntitiesResponse response = SearchEntitiesResponse.newBuilder().build();
        Mockito.when(requestExecutor.searchEntities(request)).thenReturn(response);
        dataProvider.getEntitiesByTag(tag, type);
        Mockito.verify(requests, Mockito.times(1)).entitiesByTagRequest(tag, type);
        Mockito.verify(requestExecutor, Mockito.times(1)).searchEntities(request);
    }

    /**
     * Tests that 'getTopologyDataDefinitions' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testGetTopologyDataDefinitions() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);
        Mockito.when(requestExecutor.getAllTopologyDataDefinitions(Mockito.any()))
                .thenReturn(Collections.emptyIterator());
        dataProvider.getTopologyDataDefinitions();
        Mockito.verify(requests, Mockito.times(1)).tddRequest();
        Mockito.verify(requestExecutor, Mockito.times(1))
                .getAllTopologyDataDefinitions(Mockito.any());
    }

    /**
     * Tests that 'searchEntities' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testSearchEntities() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);
        SearchEntitiesResponse response = SearchEntitiesResponse.newBuilder().build();
        Mockito.when(requestExecutor.searchEntities(Mockito.any())).thenReturn(response);
        SearchParameters searchParameters = SearchParameters.newBuilder().build();
        List<SearchParameters> searchParametersList = Collections.singletonList(searchParameters);
        dataProvider.searchEntities(searchParametersList);
        Mockito.verify(requests, Mockito.times(1)).createFilterEntityRequest(Mockito.any());
        Mockito.verify(requestExecutor, Mockito.times(1)).searchEntities(Mockito.any());
    }

    /**
     * Tests that 'searchEntities' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testGetEntitiesByOids() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);
        Set<Long> oids = Collections.emptySet();
        Mockito.when(requestExecutor.retrieveTopologyEntities(Mockito.any())).thenReturn(Collections.emptyIterator());
        dataProvider.getEntitiesByOids(oids);
        Mockito.verify(requests, Mockito.times(1)).getEntitiesByOidsRequest(oids);
        Mockito.verify(requestExecutor, Mockito.times(1)).retrieveTopologyEntities(Mockito.any());
    }

    /**
     * Tests that 'getGroupMembersIds' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testGetGroupMembersIds() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);
        long id = 1200L;
        GroupDTO.GroupID groupID = GroupDTO.GroupID.newBuilder().setId(id).build();
        Mockito.when(requestExecutor.getGroupMembers(Mockito.any())).thenReturn(Collections.emptyIterator());
        dataProvider.getGroupMembersIds(groupID);
        Mockito.verify(requests, Mockito.times(1)).getGroupMembersRequest(id);
        Mockito.verify(requestExecutor, Mockito.times(1)).getGroupMembers(Mockito.any());
    }

    /**
     * Tests that 'getGroupMembersIds' returns empty group when an exception is thrown.
     */
    @Test
    public void testGetGroupMembersMissingGroup() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);
        long id = 1200L;
        GroupDTO.GroupID groupID = GroupDTO.GroupID.newBuilder().setId(id).build();
        Mockito.when(requestExecutor.getGroupMembers(Mockito.any()))
                .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));
        Set<Long> members = dataProvider.getGroupMembersIds(groupID);
        Assert.assertTrue(members.isEmpty());
    }
}
