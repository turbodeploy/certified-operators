package com.vmturbo.mediation.udt.explore;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.LeafEntitiesRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.LeafEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.mediation.udt.TestUtils;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;

/**
 * Test class for {@link DataProvider}.
 */
public class DataProviderTest {
    private static final long ENTRY = 1L;
    private static final long CONTEXT_BASED_ENTRY = 2L;
    private static final long PROBE_ID = 1234567L;
    private static final long UDT_TARGET_ID = 100L;

    SearchFilterResolver resolver = Mockito.mock(SearchFilterResolver.class);

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
     * Tests that {@link DataProvider#getTopologyDataDefinitions()} returns context-based ATDs
     * along with other.
     */
    @Test
    public void testGetTopologyDataDefinitionsWithEnabledContextBasedAtds() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);
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

    /**
     * Tests that {@link DataProvider#getUdtTargetId()} return UDT target identifier.
     * @throws CommunicationException if any communication issues occurred
     */
    @Test
    public void testGetUdtTargetId() throws CommunicationException {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);

        ProbeInfo probeInfo = TestUtils.mockProbeInfo(PROBE_ID, SDKProbeType.UDT);
        Mockito.when(requestExecutor.findProbe(SDKProbeType.UDT))
                .thenReturn(probeInfo);
        Set<TargetInfo> mockedTargets = TestUtils.createMockedTargets(PROBE_ID, UDT_TARGET_ID);
        Mockito.when(requestExecutor.findTarget(PROBE_ID)).thenReturn(new ArrayList<>(mockedTargets));

        Long udtTargetId = dataProvider.getUdtTargetId();

        Assert.assertNotNull(udtTargetId);
        Assert.assertEquals(Long.valueOf(UDT_TARGET_ID), udtTargetId);
    }

    /**
     * Tests that {@link DataProvider#getUdtTargetId()} return {@code null}
     * if UDT probe not found.
     * @throws CommunicationException if any communication issues occurred
     */
    @Test
    public void testGetUdtTargetIdIfProbeNotFound() throws CommunicationException {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);

        Long udtTargetId = dataProvider.getUdtTargetId();

        Assert.assertNull(udtTargetId);

        Mockito.verify(requestExecutor, Mockito.never()).findTarget(PROBE_ID);
    }

    /**
     * Tests that {@link DataProvider#getUdtTargetId()} return {@code null}
     * if UDT targets not found.
     * @throws CommunicationException if any communication issues occurred
     */
    @Test
    public void testGetUdtTargetIdIfUdtTargetsNotFound() throws CommunicationException {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);

        ProbeInfo probeInfo = TestUtils.mockProbeInfo(PROBE_ID, SDKProbeType.UDT);
        Mockito.when(requestExecutor.findProbe(SDKProbeType.UDT))
                .thenReturn(probeInfo);
        Mockito.when(requestExecutor.findTarget(PROBE_ID)).thenReturn(Collections.emptyList());

        Long udtTargetId = dataProvider.getUdtTargetId();

        Assert.assertNull(udtTargetId);
    }

    /**
     * Tests that {@link DataProvider#getUdtTargetId()} returns {@code null}
     * if {@link CommunicationException} is thrown.
     * @throws CommunicationException if any communication issues occurred
     */
    @Test
    public void testGetUdtTargetIdIfCommunicationExceptionThrown() throws CommunicationException {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);

        Mockito.when(requestExecutor.findProbe(SDKProbeType.UDT))
                .thenThrow(CommunicationException.class);

        Long udtTargetId = dataProvider.getUdtTargetId();

        Assert.assertNull(udtTargetId);
    }

    /**
     * Tests {@link DataProvider#retrieveTagValues(String, EntityType)} that
     * correctly calls DataRequests.class and RequestExecutor.class.
     */
    @Test
    public void testRetrieveTagValue() {
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataRequests requests = Mockito.mock(DataRequests.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, requests, resolver);

        dataProvider.retrieveTagValues("tag1", EntityType.SERVICE);

        Mockito.verify(requests).getSearchTagValuesRequest("tag1", EntityType.SERVICE);
        Mockito.verify(requestExecutor).getTagValues(Mockito.any());
    }

    /**
     * It tests `getLeafEntities` method.
     * Tests tan empty atdContext.
     * Tests that correct OIDs sre passed to the request executor.
     */
    @Test
    public void testGetLeafEntities() {
        List<Long> oids = Lists.newArrayList(1L, 2L);
        RequestExecutor requestExecutor = Mockito.mock(RequestExecutor.class);
        DataProvider dataProvider = new DataProvider(requestExecutor, new DataRequests(), resolver);

        Set<UdtChildEntity> leaves;
        leaves = dataProvider.getLeafEntities(Collections.emptySet(), null);
        Assert.assertTrue(leaves.isEmpty());

        Set<UdtChildEntity> atdContext = new HashSet<>();
        atdContext.add(new UdtChildEntity(oids.get(0), EntityType.APPLICATION_COMPONENT));
        atdContext.add(new UdtChildEntity(oids.get(1), EntityType.CONTAINER));
        Mockito.when(requestExecutor.getLeafEntities(Mockito.any()))
                .thenReturn(LeafEntitiesResponse.newBuilder().build());

        ArgumentCaptor<LeafEntitiesRequest> reqCaptor = ArgumentCaptor.forClass(LeafEntitiesRequest.class);
        leaves = dataProvider.getLeafEntities(atdContext, null);
        Mockito.verify(requestExecutor).getLeafEntities(reqCaptor.capture());
        LeafEntitiesRequest request = reqCaptor.getValue();
        List<Long> seeds = request.getSeedsList();
        assertTrue(seeds.size() == oids.size() && seeds.containsAll(oids) && oids.containsAll(seeds));

    }
}
