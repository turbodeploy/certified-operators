package com.vmturbo.api.component.external.api.util.action;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
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

    private SearchServiceBlockingStub searchServiceGrpc;

    @Before
    public void setUp() {
        searchServiceGrpc = SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

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
        SearchUtil.populateActionApiDTOWithTargets(topologyProcessor, searchServiceGrpc, actionApiDTO);

        // "new entity" should have a discovering target; the rest of the entities should be null
        Assert.assertEquals(
            targetId, (long)Long.valueOf(actionApiDTO.getNewEntity().getDiscoveredBy().getUuid()));
        Assert.assertNull(actionApiDTO.getTarget());
        Assert.assertNull(actionApiDTO.getCurrentEntity());
    }
}
