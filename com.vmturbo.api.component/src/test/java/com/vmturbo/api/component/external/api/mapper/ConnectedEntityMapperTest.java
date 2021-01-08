package com.vmturbo.api.component.external.api.mapper;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.GraphRequest;
import com.vmturbo.common.protobuf.search.Search.GraphResponse;
import com.vmturbo.common.protobuf.search.Search.OidList;
import com.vmturbo.common.protobuf.search.Search.ResponseNode;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Test mapping of connected entities.
 */
public class ConnectedEntityMapperTest {

    private SearchServiceMole searchService = spy(new SearchServiceMole());

    private SearchServiceBlockingStub searchServiceBlockingStub;

    private RepositoryServiceGrpc.RepositoryServiceBlockingStub repositoryRpcService;

    private final RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());

    private final long realTimeTopologyContextId = 7777777L;

    private final long regionOid = 888L;

    private final long cloudCommitmentOid = 12345678L;

    private ConnectedEntityMapper connectedEntityMapper;

    private final String regionDisplayName = "us-west-1";

    private final long businessAccountOid = 654321L;

    private final String businessAccountDisplayName = "Aws-Master";

    /**
     * Rule for mock server.
     */
    @Rule
    public GrpcTestServer mockServer = GrpcTestServer.newServer(searchService, repositoryService);

    private final ConnectedEntity connectedEntity = ConnectedEntity.newBuilder().setConnectedEntityType(EntityType.REGION.getValue()).setConnectedEntityId(regionOid).build();

    private final TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(cloudCommitmentOid)
            .setEntityType(ApiEntityType.CLOUD_COMMITMENT.typeNumber())
            .addConnectedEntityList(0, connectedEntity)
            .build();

    private final MinimalEntity region = MinimalEntity.newBuilder().setOid(regionOid)
            .setEntityType(ApiEntityType.REGION.typeNumber())
            .setDisplayName(regionDisplayName)
            .build();

    private final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setOid(businessAccountOid).setDisplayName(businessAccountDisplayName)
            .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber()).build();

    final MinimalEntity businessAccountMinimalEntity = MinimalEntity.newBuilder()
            .setOid(businessAccountOid)
            .setEntityType(EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE)
            .setDisplayName(businessAccountDisplayName)
            .build();

    /**
     * Setuo the test.
     *
     * @throws IOException An IO Exception.
     */
    @Before
    public void setup() throws IOException {
        searchServiceBlockingStub = SearchServiceGrpc.newBlockingStub(mockServer.getChannel());
        repositoryRpcService = RepositoryServiceGrpc.newBlockingStub(mockServer.getChannel());
        mockServer.start();
        connectedEntityMapper = new ConnectedEntityMapper(repositoryRpcService, realTimeTopologyContextId, searchServiceBlockingStub);
        when(repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityOids(regionOid)
                .setTopologyContextId(realTimeTopologyContextId)
                .setTopologyType(TopologyType.SOURCE)
                .build()
        )).thenReturn(Arrays.asList(PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(region).build()).build()));

        when(searchService.graphSearch(GraphRequest.newBuilder().addOids(cloudCommitmentOid).putNodes("BusinessAccount",
                SearchProtoUtil.node(Type.MINIMAL, TraversalDirection.OWNED_BY, EntityDTO.EntityType.BUSINESS_ACCOUNT).build()).build()))
                .thenReturn(GraphResponse.newBuilder()
                        .putNodes("BusinessAccount", ResponseNode.newBuilder().putOidMap(cloudCommitmentOid, OidList
                                .newBuilder().addOids(businessAccountOid).build())
                                .putEntities(businessAccountOid, PartialEntity.newBuilder().setMinimal(businessAccountMinimalEntity).build()).build()).build());
    }

    /**
     * Test mapping of connected entities.
     */
    @Test
    public void testMappingConnectedEntities() {
        List<BaseApiDTO> listOfConnectedEntities = new ArrayList<>(connectedEntityMapper.mapConnectedEntities(topologyEntityDTO));

        assert (listOfConnectedEntities.size() == 2);
        // The first item in the list should be a region
        BaseApiDTO regionDTO = listOfConnectedEntities.get(0);
        assert (regionDTO.getDisplayName().equals(regionDisplayName));
        assert (regionDTO.getUuid()).equals(Long.toString(regionOid));

        // The 2nd item in the list should be a business account.
        BaseApiDTO account = listOfConnectedEntities.get(1);
        assert (account.getDisplayName().equals(businessAccountDisplayName));
        assert (account.getUuid()).equals(Long.toString(businessAccountOid));
    }
}
