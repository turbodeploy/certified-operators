package com.vmturbo.mediation.udt.explore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.mediation.udt.UdtProbe;

/**
 * Test class for {@link DataProvider}.
 */
public class DataProviderTest {
    private static final long ENTRY = 1L;
    private static final long CONTEXT_BASED_ENTRY = 2L;

    SearchFilterResolver resolver = Mockito.mock(SearchFilterResolver.class);

    /**
     * Tests that 'getTopologyDataDefinitions' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testGetTopologyDataDefinitions() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver,
                UdtProbe.buildTopologyDefinitionsFilter(false));
        Mockito.when(requestExecutor.getAllTopologyDataDefinitions(Mockito.any()))
                .thenReturn(Collections.emptyIterator());
        dataProvider.getTopologyDataDefinitions();
        Mockito.verify(requests, Mockito.times(1)).tddRequest();
        Mockito.verify(requestExecutor, Mockito.times(1))
                .getAllTopologyDataDefinitions(Mockito.any());
    }

    /**
     * Tests that {@link DataProvider#getTopologyDataDefinitions()} returns context-based ATDs
     * along with other.
     */
    @Test
    public void testGetTopologyDataDefinitionsWithEnabledContextBasedAtds() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver,
                UdtProbe.buildTopologyDefinitionsFilter(true));
        Mockito.when(requestExecutor.getAllTopologyDataDefinitions(Mockito.any()))
                .thenReturn(buildTopologyDataDefinitionResponseIterator());
        Map<Long, TopologyDataDefinition> topologyDataDefinitions = dataProvider.getTopologyDataDefinitions();
        Mockito.verify(requests, Mockito.times(1)).tddRequest();
        Mockito.verify(requestExecutor, Mockito.times(1))
                .getAllTopologyDataDefinitions(Mockito.any());

        Assert.assertNotNull(topologyDataDefinitions);
        Assert.assertEquals(2, topologyDataDefinitions.size());
        Assert.assertTrue(topologyDataDefinitions.containsKey(ENTRY));
        Assert.assertTrue(topologyDataDefinitions.containsKey(CONTEXT_BASED_ENTRY));
    }

    /**
     * Tests that {@link DataProvider#getTopologyDataDefinitions()} does not return
     * context-based ATDs.
     */
    @Test
    public void testGetTopologyDataDefinitionsWithDisabledContextBasedAtds() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver,
                UdtProbe.buildTopologyDefinitionsFilter(false));
        Mockito.when(requestExecutor.getAllTopologyDataDefinitions(Mockito.any()))
                .thenReturn(buildTopologyDataDefinitionResponseIterator());
        Map<Long, TopologyDataDefinition> topologyDataDefinitions = dataProvider.getTopologyDataDefinitions();
        Mockito.verify(requests, Mockito.times(1)).tddRequest();
        Mockito.verify(requestExecutor, Mockito.times(1))
                .getAllTopologyDataDefinitions(Mockito.any());

        Assert.assertNotNull(topologyDataDefinitions);
        Assert.assertEquals(1, topologyDataDefinitions.size());
        Assert.assertTrue(topologyDataDefinitions.containsKey(ENTRY));
    }

    private static Iterator<GetTopologyDataDefinitionResponse> buildTopologyDataDefinitionResponseIterator() {
        List<GetTopologyDataDefinitionResponse> responses = new ArrayList<>();
        responses.add(buildResponse(ENTRY, false));
        responses.add(buildResponse(CONTEXT_BASED_ENTRY, true));
        return responses.iterator();
    }

    private static GetTopologyDataDefinitionResponse buildResponse(long id, boolean contextBased) {
        ManualEntityDefinition manualEntityDefinition = ManualEntityDefinition.newBuilder()
                .setContextBased(contextBased)
                .build();
        TopologyDataDefinition topologyDataDefinition = TopologyDataDefinition.newBuilder()
                .setManualEntityDefinition(manualEntityDefinition)
                .build();
        TopologyDataDefinitionEntry topologyDataDefinitionEntry = TopologyDataDefinitionEntry.newBuilder()
                .setId(id)
                .setDefinition(topologyDataDefinition)
                .build();
        return GetTopologyDataDefinitionResponse.newBuilder()
                .setTopologyDataDefinition(topologyDataDefinitionEntry)
                .build();
    }

    /**
     * Tests that 'searchEntities' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testSearchEntities() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver,
                UdtProbe.buildTopologyDefinitionsFilter(false));
        SearchEntitiesResponse response = SearchEntitiesResponse.newBuilder().build();
        Mockito.when(requestExecutor.searchEntities(Mockito.any())).thenReturn(response);
        SearchParameters searchParameters = SearchParameters.newBuilder().build();
        List<SearchParameters> searchParametersList = Collections.singletonList(searchParameters);
        dataProvider.searchEntities(searchParametersList);
        Mockito.verify(requests, Mockito.times(1)).createFilterEntityRequest(Mockito.any());
        Mockito.verify(requestExecutor, Mockito.times(1)).searchEntities(Mockito.any());
    }

    /**
     * Tests that 'getGroupMembersIds' correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testGetGroupMembersIds() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver,
                UdtProbe.buildTopologyDefinitionsFilter(false));
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
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver,
                UdtProbe.buildTopologyDefinitionsFilter(false));
        long id = 1200L;
        GroupDTO.GroupID groupID = GroupDTO.GroupID.newBuilder().setId(id).build();
        Mockito.when(requestExecutor.getGroupMembers(Mockito.any()))
                .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));
        Set<Long> members = dataProvider.getGroupMembersIds(groupID);
        Assert.assertTrue(members.isEmpty());
    }
}
