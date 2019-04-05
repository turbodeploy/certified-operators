package com.vmturbo.api.component.external.api.util.action;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Test methods in {@link SearchUtil}.
 */
public class SearchUtilTest {
    private final SearchServiceMole searchService = spy(new SearchServiceMole());

    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(searchService);

    @Captor
    private ArgumentCaptor<List<String>> stringListCaptor;

    private SearchUtil searchUtil;
    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
    private final SupplyChainFetcherFactory supplyChainFetcherFactory =
        mock(SupplyChainFetcherFactory.class);

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        searchUtil =
            new SearchUtil(
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                topologyProcessor,
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                mock(ActionSpecMapper.class),
                mock(PaginationMapper.class),
                supplyChainFetcherFactory,
                0L);
    }


    /**
     * Tests {@link SearchUtil#populateActionApiDTOWithTargets}.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testPopulateActionApiDTOWithTargets() throws Exception {
        // set up an action containing one entity as its "new entity"
        final ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
        final long entityId = 1L;
        serviceEntityApiDTO.setUuid(Long.toString(entityId));
        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        actionApiDTO.setNewEntity(serviceEntityApiDTO);
        final long targetId = 2L;
        final long probeId = 3L;
        final String probeType = "probeType";
        final TargetInfo targetInfo = mock(TargetInfo.class);
        final ProbeInfo probeInfo = mock(ProbeInfo.class);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(probeInfo.getType()).thenReturn(probeType);

        // mock service behavior
        when(searchService.searchTopologyEntityDTOs(
                eq(SearchTopologyEntityDTOsRequest.newBuilder().addEntityOid(entityId).build())))
            .thenReturn(
                SearchTopologyEntityDTOsResponse.newBuilder()
                    .addTopologyEntityDtos(
                        TopologyEntityDTO.newBuilder()
                            .setOid(entityId)
                            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .setOrigin(
                                Origin.newBuilder()
                                    .setDiscoveryOrigin(
                                        DiscoveryOrigin.newBuilder()
                                            .addDiscoveringTargetIds(targetId)
                                            .build()))
                            .build())
                    .build());
        when(topologyProcessor.getTarget(eq(targetId))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(eq(probeId))).thenReturn(probeInfo);

        // call the service
        searchUtil.populateActionApiDTOWithTargets(actionApiDTO);

        // "new entity" should have a discovering target; the rest of the entities should be null
        Assert.assertEquals(
            targetId, (long)Long.valueOf(actionApiDTO.getNewEntity().getDiscoveredBy().getUuid()));
        Assert.assertNull(actionApiDTO.getTarget());
        Assert.assertNull(actionApiDTO.getCurrentEntity());
    }

    /**
     * Tests that, when {@link SearchUtil#expandScope} is called, a supply chain fetcher
     * will be created, that the two requirements will be passed correctly to it, and that
     * the method {@link SupplyChainNodeFetcherBuilder#fetch} will be called.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testExpandScopeCheckServiceCalls() throws Exception {
        // input
        final Set<Long> seedsIds = ImmutableSet.of(1L, 2L, 3L);
        final Set<String> seedsStringIds = ImmutableSet.of("1", "2", "3");
        final List<String> relatedEntityTypes = Collections.singletonList("VirtualMachine");

        // mocks
        final SupplyChainNodeFetcherBuilder fetcherBuilder = mock(SupplyChainNodeFetcherBuilder.class);
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(fetcherBuilder.addSeedUuids(any())).thenReturn(fetcherBuilder);
        when(fetcherBuilder.entityTypes(any())).thenReturn(fetcherBuilder);

        // call service
        searchUtil.expandScope(seedsIds, relatedEntityTypes);

        // verify calls have been made on the builder
        verify(fetcherBuilder).addSeedUuids(stringListCaptor.capture());
        verify(fetcherBuilder).entityTypes(eq(relatedEntityTypes));
        verify(fetcherBuilder).fetchEntityIds();
        final List<String> seedsArgument = stringListCaptor.getValue();
        Assert.assertEquals(seedsStringIds, seedsArgument.stream().collect(Collectors.toSet()));
    }
}
