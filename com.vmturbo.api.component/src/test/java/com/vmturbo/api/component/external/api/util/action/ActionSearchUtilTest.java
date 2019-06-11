package com.vmturbo.api.component.external.api.util.action;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Test methods in {@link ActionSearchUtil}.
 */
public class ActionSearchUtilTest {
    private final SearchServiceMole searchService = spy(new SearchServiceMole());
    private final RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private final ActionsServiceMole actionsService = spy(new ActionsServiceMole());

    @Rule
    public final GrpcTestServer grpcServer =
            GrpcTestServer.newServer(searchService, repositoryService, actionsService);

    private ActionSearchUtil actionSearchUtil;
    private final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
    private final SupplyChainFetcherFactory supplyChainFetcherFactory =
        mock(SupplyChainFetcherFactory.class);

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        final RepositoryApi repositoryApi =
                new RepositoryApi(
                        mock(SeverityPopulator.class),
                        RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        SearchServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        topologyProcessor,
                        0L);
        actionSearchUtil =
            new ActionSearchUtil(
                ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                mock(ActionSpecMapper.class),
                mock(PaginationMapper.class),
                supplyChainFetcherFactory,
                repositoryApi,
                0L);
    }

    /**
     * Tests {@link ActionSearchUtil#populateActionApiDTOWithTargets}.
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
        // this represents the case where we have a default service entity object as the current
        // entity.  This can happen, for example, for a START action
        actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
        final long targetId = 2L;
        final long probeId = 3L;
        final String probeType = "probeType";
        final TargetInfo targetInfo = mock(TargetInfo.class);
        final ProbeInfo probeInfo = mock(ProbeInfo.class);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(probeInfo.getType()).thenReturn(probeType);
        final TargetApiDTO discoveringTarget = new TargetApiDTO();
        discoveringTarget.setUuid(Long.toString(targetId));
        serviceEntityApiDTO.setDiscoveredBy(discoveringTarget);

        // mock service behavior
        when(topologyProcessor.getTarget(eq(targetId))).thenReturn(targetInfo);
        when(topologyProcessor.getProbe(eq(probeId))).thenReturn(probeInfo);

        // call the service
        actionSearchUtil.populateActionApiDTOWithTargets(actionApiDTO);

        // "new entity" should have a discovering target; the rest of the entities should be null
        Assert.assertEquals(
            targetId, (long)Long.valueOf(actionApiDTO.getNewEntity().getDiscoveredBy().getUuid()));
        Assert.assertEquals(probeType, actionApiDTO.getNewEntity().getDiscoveredBy().getType());
        Assert.assertNull(actionApiDTO.getTarget());
        Assert.assertNull(actionApiDTO.getCurrentEntity().getDiscoveredBy());
    }
}
