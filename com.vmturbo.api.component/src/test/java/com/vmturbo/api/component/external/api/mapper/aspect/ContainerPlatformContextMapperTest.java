package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.ContainerPlatformContextAspectMapper.ContainerPlatformContextMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.ContainerPlatformContextAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ContainerPlatformContextMapper}.
 */
public class ContainerPlatformContextMapperTest {
    private static Set<Integer> cloudNativeEntityConnections
            = ImmutableSet.of(ApiEntityType.NAMESPACE.typeNumber(),
            ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber());

    private ContainerPlatformContextMapper contextMapper;

    private final long realTimeTopologyContextId = 7777777L;
    private final long planTimeTopologyContextId = 123L;
    private RepositoryServiceGrpc.RepositoryServiceBlockingStub repositoryRpcService;
    private SupplyChainServiceBlockingStub supplyChainRpcService;
    private final SupplyChainServiceMole supplyChainService = spy(new SupplyChainServiceMole());

    private final long namespaceOid = 888L;
    private final String nsDisplayName = "turbo";
    private final MinimalEntity namespace = MinimalEntity.newBuilder()
            .setOid(namespaceOid)
            .setEntityType(ApiEntityType.NAMESPACE.typeNumber())
            .setDisplayName(nsDisplayName)
            .build();
    private final BaseApiDTO namespaceEntity = ServiceEntityMapper.toBaseApiDTO(namespace);

    private final long secondNamespaceOid = 889L;
    private final String secondNsDisplayName = "turbo";
    private final MinimalEntity secondNamespace = MinimalEntity.newBuilder()
            .setOid(secondNamespaceOid)
            .setEntityType(ApiEntityType.NAMESPACE.typeNumber())
            .setDisplayName(secondNsDisplayName)
            .build();

    private final long clusterOid = 999L;
    private final String clusterDisplayName = "kube-cluster";
    private final MinimalEntity containerCluster = MinimalEntity.newBuilder().setOid(clusterOid)
            .setEntityType(ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber())
            .setDisplayName(clusterDisplayName)
            .build();
    private final BaseApiDTO containerClusterEntity = ServiceEntityMapper.toBaseApiDTO(containerCluster);

    private final ApiPartialEntity container = ApiPartialEntity.newBuilder().setOid(11L)
            .setEntityType(ApiEntityType.CONTAINER.typeNumber())
            .build();
    private final ApiPartialEntity pod = ApiPartialEntity.newBuilder().setOid(21L)
            .setEntityType(ApiEntityType.CONTAINER_POD.typeNumber())
            .build();
    private final ApiPartialEntity secondPod = ApiPartialEntity.newBuilder().setOid(22L)
            .setEntityType(ApiEntityType.CONTAINER_POD.typeNumber())
            .build();
    private final ApiPartialEntity containerSpec = ApiPartialEntity.newBuilder().setOid(31L)
            .setEntityType(ApiEntityType.CONTAINER_SPEC.typeNumber())
            .build();
    private final ApiPartialEntity controller = ApiPartialEntity.newBuilder().setOid(41L)
            .setEntityType(ApiEntityType.WORKLOAD_CONTROLLER.typeNumber())
            .build();

    private final SupplyChainSeed containerSeed = SupplyChainSeed.newBuilder()
            .setSeedOid(container.getOid())
            .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(container.getOid())
                    .addAllEntityTypesToInclude(cloudNativeEntityConnections))
            .build();
    private final SupplyChainSeed podSeed = SupplyChainSeed.newBuilder()
            .setSeedOid(pod.getOid())
            .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(pod.getOid())
                    .addAllEntityTypesToInclude(cloudNativeEntityConnections))
            .build();
    private final SupplyChainSeed secondPodSeed = SupplyChainSeed.newBuilder()
            .setSeedOid(secondPod.getOid())
            .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(secondPod.getOid())
                    .addAllEntityTypesToInclude(cloudNativeEntityConnections))
            .build();

    private final SupplyChainSeed containerSpecSeed = SupplyChainSeed.newBuilder()
            .setSeedOid(containerSpec.getOid())
            .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(containerSpec.getOid())
                    .addAllEntityTypesToInclude(cloudNativeEntityConnections))
            .build();

    private final SupplyChainSeed controllerSeed = SupplyChainSeed.newBuilder()
            .setSeedOid(controller.getOid())
            .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(controller.getOid())
                    .addAllEntityTypesToInclude(cloudNativeEntityConnections))
            .build();

    private final SupplyChainNode namespaceNode = SupplyChainNode.newBuilder()
            .setEntityType(ApiEntityType.NAMESPACE.typeNumber())
            .putMembersByState(1, MemberList.newBuilder().addMemberOids(namespaceOid).build())
            .build();
    private final SupplyChainNode secondNamespaceNode = SupplyChainNode.newBuilder()
            .setEntityType(ApiEntityType.NAMESPACE.typeNumber())
            .putMembersByState(1, MemberList.newBuilder().addMemberOids(secondNamespaceOid).build())
            .build();

    private final SupplyChainNode clusterNode = SupplyChainNode.newBuilder()
            .setEntityType(ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber())
            .putMembersByState(1, MemberList.newBuilder().addMemberOids(clusterOid).build())
            .build();

    private final SupplyChain supplyChain = SupplyChain.newBuilder()
            .addSupplyChainNodes(namespaceNode)
            .addSupplyChainNodes(clusterNode)
            .build();

    private final SupplyChain secondSupplyChain = SupplyChain.newBuilder()
            .addSupplyChainNodes(secondNamespaceNode)
            .addSupplyChainNodes(clusterNode)
            .build();

    /**
     * Rule for mock server.
     */
    @Rule
    public GrpcTestServer mockServer = GrpcTestServer.newServer(supplyChainService);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    /**
     * Set up before test.
     * @throws IOException  IO Exception during the test
     */
    @Before
    public void setup() throws IOException {
        repositoryRpcService = RepositoryServiceGrpc.newBlockingStub(mockServer.getChannel());
        supplyChainRpcService = SupplyChainServiceGrpc.newBlockingStub(mockServer.getChannel());
        mockServer.start();
        contextMapper = new ContainerPlatformContextMapper(supplyChainRpcService,
                repositoryApi,
                realTimeTopologyContextId);
    }

    /**
     * Test that mapping entities that have no namespace/cluster in the supply chain does not
     * result in additional API calls to the repository to retrieve entities. Guards against
     * accidentally retrieving all entities.
     */
    @Test
    public void testNoEntitiesRetrievedIfNothingInSeed() {
        when(supplyChainService.getMultiSupplyChains(any()))
            .thenReturn(Collections.emptyList());
        Collection<ApiPartialEntity> entities = Arrays.asList(container);

        Map<Long, EntityAspect> entityAspects
                = contextMapper.bulkMapContainerPlatformContext(entities, Optional.empty());

        assertTrue(entityAspects.isEmpty());
        verifyZeroInteractions(repositoryApi);
    }

    /**
     * Test container platform context data for set of ApiPartial entities belonging
     * to the same namespace and cluster.
     */
    @Test
    public void testMappingContextForMultipleEntities() {
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(namespace, containerCluster));
        when(repositoryApi.entitiesRequest(Sets.newSet(clusterOid, namespaceOid)))
            .thenReturn(req);

        final GetMultiSupplyChainsRequest.Builder requestBuilder =
                GetMultiSupplyChainsRequest.newBuilder()
                        .setContextId(realTimeTopologyContextId);

        requestBuilder.addSeeds(containerSeed)
                .addSeeds(podSeed)
                .addSeeds(containerSpecSeed)
                .addSeeds(controllerSeed);

        when(supplyChainService.getMultiSupplyChains(requestBuilder.build()))
                .thenReturn(Arrays.asList(
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(container.getOid())
                                .setSupplyChain(supplyChain)
                                .build(),
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(pod.getOid())
                                .setSupplyChain(supplyChain)
                                .build(),
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(containerSpec.getOid())
                                .setSupplyChain(supplyChain)
                                .build(),
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(controller.getOid())
                                .setSupplyChain(supplyChain)
                                .build()

                ));

        Collection<ApiPartialEntity> entities = Arrays.asList(container, pod, containerSpec, controller);

        Map<Long, EntityAspect> entityAspects
                = contextMapper.bulkMapContainerPlatformContext(entities, Optional.empty());

        Assert.assertEquals(4, entityAspects.size());

        ContainerPlatformContextAspectApiDTO context;

        EntityAspect containerContext = entityAspects.get(container.getOid());
        assertTrue(containerContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)containerContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect podContext = entityAspects.get(pod.getOid());
        assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect containerSpecContext = entityAspects.get(containerSpec.getOid());
        assertTrue(containerSpecContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)containerSpecContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect controllerContext = entityAspects.get(controller.getOid());
        assertTrue(controllerContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)controllerContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }

    /**
     * Test that we can correctly map the context for entities in plans.
     */
    @Test
    public void testMappingContextForPlanEntities() {
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(namespace, containerCluster));
        when(repositoryApi.entitiesRequest(Sets.newSet(clusterOid, namespaceOid)))
            .thenReturn(req);

        final GetMultiSupplyChainsRequest.Builder requestBuilder =
            GetMultiSupplyChainsRequest.newBuilder()
                .setContextId(planTimeTopologyContextId);

        requestBuilder.addSeeds(containerSeed)
            .addSeeds(podSeed)
            .addSeeds(containerSpecSeed)
            .addSeeds(controllerSeed);

        when(supplyChainService.getMultiSupplyChains(requestBuilder.build()))
            .thenReturn(Arrays.asList(
                GetMultiSupplyChainsResponse.newBuilder()
                    .setSeedOid(container.getOid())
                    .setSupplyChain(supplyChain)
                    .build(),
                GetMultiSupplyChainsResponse.newBuilder()
                    .setSeedOid(pod.getOid())
                    .setSupplyChain(supplyChain)
                    .build(),
                GetMultiSupplyChainsResponse.newBuilder()
                    .setSeedOid(containerSpec.getOid())
                    .setSupplyChain(supplyChain)
                    .build(),
                GetMultiSupplyChainsResponse.newBuilder()
                    .setSeedOid(controller.getOid())
                    .setSupplyChain(supplyChain)
                    .build()

            ));

        Collection<ApiPartialEntity> entities = Arrays.asList(container, pod, containerSpec, controller);

        Map<Long, EntityAspect> entityAspects
            = contextMapper.bulkMapContainerPlatformContext(entities, Optional.of(planTimeTopologyContextId));

        Assert.assertEquals(4, entityAspects.size());

        ContainerPlatformContextAspectApiDTO context;

        EntityAspect containerContext = entityAspects.get(container.getOid());
        assertTrue(containerContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)containerContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect podContext = entityAspects.get(pod.getOid());
        assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect containerSpecContext = entityAspects.get(containerSpec.getOid());
        assertTrue(containerSpecContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)containerSpecContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect controllerContext = entityAspects.get(controller.getOid());
        assertTrue(controllerContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)controllerContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }

    /**
     * Test that we can correctly map the context for entities that are created
     * as a result of a provision action in a plan.
     */
    @Test
    public void testMappingContextForProvisionedPlanEntities() {
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(namespace, containerCluster));
        when(repositoryApi.entitiesRequest(Sets.newSet(clusterOid, namespaceOid)))
            .thenReturn(req);

        final GetMultiSupplyChainsRequest.Builder requestBuilder =
            GetMultiSupplyChainsRequest.newBuilder()
                .setContextId(planTimeTopologyContextId)
                .addSeeds(containerSeed);

        final long cloneId = 997983234L;
        final long secondCloneId = 997983235L;
        when(supplyChainService.getMultiSupplyChains(requestBuilder.build()))
            .thenReturn(Collections.singletonList(
                GetMultiSupplyChainsResponse.newBuilder()
                    .setSeedOid(container.getOid())
                    .setSupplyChain(supplyChain)
                    .build()));

        final TopologyEntityDTO originalContainer = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setOid(container.getOid())
            .build();
        final TopologyEntityDTO clone = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setOid(cloneId)
            .setOrigin(Origin.newBuilder().setAnalysisOrigin(
                AnalysisOrigin.newBuilder().setOriginalEntityId(container.getOid())))
            .build();
        final TopologyEntityDTO secondClone = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setOid(secondCloneId)
            .setOrigin(Origin.newBuilder().setAnalysisOrigin(
                AnalysisOrigin.newBuilder().setOriginalEntityId(container.getOid())))
            .build();
        final Collection<TopologyEntityDTO> entities = Arrays.asList(originalContainer, clone, secondClone);

        final Map<Long, EntityAspect> entityAspects = contextMapper
            .getContainerPlatformContext(entities, Optional.of(planTimeTopologyContextId));

        Assert.assertEquals(3, entityAspects.size());

        ContainerPlatformContextAspectApiDTO context;

        EntityAspect containerContext = entityAspects.get(container.getOid());
        assertTrue(containerContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)containerContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect cloneContext = entityAspects.get(cloneId);
        assertTrue(cloneContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)cloneContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect secondCloneContext = entityAspects.get(secondCloneId);
        assertTrue(secondCloneContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)secondCloneContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }

    /**
     * Test container platform context data for set of ApiPartial entities belonging
     * to the different namespaces and same cluster.
     */
    @Test
    public void testMappingContextForEntitiesInDifferentNamespaces() {
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(namespace, secondNamespace, containerCluster));
        when(repositoryApi.entitiesRequest(
            Sets.newSet(clusterOid, namespaceOid, secondNamespaceOid)))
            .thenReturn(req);

        final GetMultiSupplyChainsRequest.Builder requestBuilder =
                GetMultiSupplyChainsRequest.newBuilder()
                        .setContextId(realTimeTopologyContextId);
        requestBuilder.addSeeds(podSeed).addSeeds(secondPodSeed);

        when(supplyChainService.getMultiSupplyChains(requestBuilder.build()))
                .thenReturn(Arrays.asList(
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(pod.getOid())
                                .setSupplyChain(supplyChain)
                                .build(),
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(secondPod.getOid())
                                .setSupplyChain(secondSupplyChain)
                                .build()
                ));

        Map<Long, EntityAspect> entityAspects
                = contextMapper.bulkMapContainerPlatformContext(Arrays.asList(pod, secondPod), Optional.empty());

        assert (entityAspects.size() == 2);
        ContainerPlatformContextAspectApiDTO context;

        EntityAspect secondPodContext = entityAspects.get(secondPod.getOid());
        assertTrue(secondPodContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)secondPodContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect podContext = entityAspects.get(pod.getOid());
        assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }

    /**
     * Test container platform context data for set of ApiPartial entities belonging
     * to the same namespace and cluster.
     */
    @Test
    public void testMappingContextForTopologyEntityDtos() {
        TopologyEntityDTO podEntityDto = TopologyEntityDTO.newBuilder().setOid(91L)
                .setEntityType(ApiEntityType.CONTAINER_POD.typeNumber())
                .build();
        SupplyChainSeed podEntitySeed = SupplyChainSeed.newBuilder()
                .setSeedOid(podEntityDto.getOid())
                .setScope(SupplyChainScope.newBuilder()
                        .addStartingEntityOid(podEntityDto.getOid())
                        .addAllEntityTypesToInclude(cloudNativeEntityConnections))
                .build();

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(namespace, containerCluster));
        when(repositoryApi.entitiesRequest(Sets.newSet(clusterOid, namespaceOid)))
            .thenReturn(req);

        final GetMultiSupplyChainsRequest.Builder requestBuilder =
                GetMultiSupplyChainsRequest.newBuilder()
                        .setContextId(realTimeTopologyContextId);
        requestBuilder.addSeeds(podEntitySeed);

        when(supplyChainService.getMultiSupplyChains(requestBuilder.build()))
                .thenReturn(Arrays.asList(
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(podEntityDto.getOid())
                                .setSupplyChain(supplyChain)
                                .build()
                ));

        Map<Long, EntityAspect> entityAspects
                = contextMapper.getContainerPlatformContext(Arrays.asList(podEntityDto), Optional.empty());

        assert (entityAspects.size() == 1);
        ContainerPlatformContextAspectApiDTO context;

        EntityAspect podContext = entityAspects.get(podEntityDto.getOid());
        assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
        Assert.assertNotNull(context.getNamespaceEntity());
        Assert.assertEquals(namespaceEntity.toString(), context.getNamespaceEntity().toString());
        Assert.assertNotNull(context.getContainerClusterEntity());
        Assert.assertEquals(containerClusterEntity.toString(), context.getContainerClusterEntity().toString());
    }

    /**
     * Test container platform context data for set of ApiPartial entities belonging
     * to the different namespaces and same cluster.
     */
    @Test
    public void testMissingContextinfo() {
        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Arrays.asList(containerCluster));
        when(repositoryApi.entitiesRequest(Sets.newSet(clusterOid, namespaceOid)))
            .thenReturn(req);

        final GetMultiSupplyChainsRequest.Builder requestBuilder =
                GetMultiSupplyChainsRequest.newBuilder()
                        .setContextId(realTimeTopologyContextId);
        requestBuilder.addSeeds(podSeed);

        when(supplyChainService.getMultiSupplyChains(requestBuilder.build()))
                .thenReturn(Arrays.asList(
                        GetMultiSupplyChainsResponse.newBuilder()
                                .setSeedOid(pod.getOid())
                                .setSupplyChain(supplyChain)
                                .build()
                ));

        Map<Long, EntityAspect> entityAspects
                = contextMapper.bulkMapContainerPlatformContext(Arrays.asList(pod), Optional.empty());

        assert (entityAspects.size() == 1);
        ContainerPlatformContextAspectApiDTO context;

        EntityAspect podContext = entityAspects.get(pod.getOid());
        assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertNull(context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }
}
