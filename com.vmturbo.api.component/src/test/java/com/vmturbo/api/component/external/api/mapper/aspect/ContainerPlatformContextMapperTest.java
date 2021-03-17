package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.aspect.ContainerPlatformContextAspectMapper.ContainerPlatformContextMapper;
import com.vmturbo.api.dto.entityaspect.ContainerPlatformContextAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for {@link ContainerPlatformContextMapper}.
 */
public class ContainerPlatformContextMapperTest {
    private static Set<Integer> cloudNativeEntityConnections
            = ImmutableSet.of(ApiEntityType.NAMESPACE.typeNumber(),
            ApiEntityType.CONTAINER_PLATFORM_CLUSTER.typeNumber());

    private ContainerPlatformContextMapper contextMapper;

    private final long realTimeTopologyContextId = 7777777L;
    private RepositoryServiceGrpc.RepositoryServiceBlockingStub repositoryRpcService;
    private SupplyChainServiceBlockingStub supplyChainRpcService;
    private final RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());
    private final SupplyChainServiceMole supplyChainService = spy(new SupplyChainServiceMole());

    private final long namespaceOid = 888L;
    private final String nsDisplayName = "turbo";
    private final MinimalEntity namespace = MinimalEntity.newBuilder()
            .setOid(namespaceOid)
            .setEntityType(ApiEntityType.NAMESPACE.typeNumber())
            .setDisplayName(nsDisplayName)
            .build();

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
    public GrpcTestServer mockServer = GrpcTestServer.newServer(supplyChainService, repositoryService);

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
                repositoryRpcService,
                realTimeTopologyContextId);
    }

    /**
     * Test container platform context data for set of ApiPartial entities belonging
     * to the same namespace and cluster.
     */
    @Test
    public void testMappingContextForMultipleEntities() {
        when(repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityOids(clusterOid)
                .addEntityOids(namespaceOid)
                .setTopologyContextId(realTimeTopologyContextId)
                .setTopologyType(TopologyType.SOURCE)
                .setReturnType(Type.MINIMAL)
                .build()
        )).thenReturn(Arrays.asList(
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(namespace)).build(),
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(containerCluster)).build())
        );

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
                = contextMapper.bulkMapContainerPlatformContext(entities);

        Assert.assertEquals(4, entityAspects.size());

        ContainerPlatformContextAspectApiDTO context;

        EntityAspect containerContext = entityAspects.get(container.getOid());
        Assert.assertTrue(containerContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)containerContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect podContext = entityAspects.get(pod.getOid());
        Assert.assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect containerSpecContext = entityAspects.get(containerSpec.getOid());
        Assert.assertTrue(containerSpecContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)containerSpecContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect controllerContext = entityAspects.get(controller.getOid());
        Assert.assertTrue(controllerContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)controllerContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }

    /**
     * Test container platform context data for set of ApiPartial entities belonging
     * to the different namespaces and same cluster.
     */
    @Test
    public void testMappingContextForEntitiesInDifferentNamespaces() {
        when(repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityOids(clusterOid)
                .addEntityOids(namespaceOid)
                .addEntityOids(secondNamespaceOid)
                .setTopologyContextId(realTimeTopologyContextId)
                .setTopologyType(TopologyType.SOURCE)
                .setReturnType(Type.MINIMAL)
                .build()
        )).thenReturn(Arrays.asList(
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(namespace)).build(),
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(secondNamespace)).build(),
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(containerCluster)).build())
        );

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
                = contextMapper.bulkMapContainerPlatformContext(Arrays.asList(pod, secondPod));

        assert (entityAspects.size() == 2);
        ContainerPlatformContextAspectApiDTO context;

        EntityAspect secondPodContext = entityAspects.get(secondPod.getOid());
        Assert.assertTrue(secondPodContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)secondPodContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());

        EntityAspect podContext = entityAspects.get(pod.getOid());
        Assert.assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
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

        when(repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityOids(clusterOid)
                .addEntityOids(namespaceOid)
                .setTopologyContextId(realTimeTopologyContextId)
                .setTopologyType(TopologyType.SOURCE)
                .setReturnType(Type.MINIMAL)
                .build()
        )).thenReturn(Arrays.asList(
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(namespace)).build(),
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(containerCluster)).build())
        );

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
                = contextMapper.getContainerPlatformContext(Arrays.asList(podEntityDto));

        assert (entityAspects.size() == 1);
        ContainerPlatformContextAspectApiDTO context;

        EntityAspect podContext = entityAspects.get(podEntityDto.getOid());
        Assert.assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertEquals(namespace.getDisplayName(), context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }

    /**
     * Test container platform context data for set of ApiPartial entities belonging
     * to the different namespaces and same cluster.
     */
    @Test
    public void testMissingContextinfo() {
        when(repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityOids(clusterOid)
                .addEntityOids(namespaceOid)
                .setTopologyContextId(realTimeTopologyContextId)
                .setTopologyType(TopologyType.SOURCE)
                .setReturnType(Type.MINIMAL)
                .build()
        )).thenReturn(Arrays.asList(
                PartialEntityBatch.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(containerCluster)).build())
        );

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
                = contextMapper.bulkMapContainerPlatformContext(Arrays.asList(pod));

        assert (entityAspects.size() == 1);
        ContainerPlatformContextAspectApiDTO context;

        EntityAspect podContext = entityAspects.get(pod.getOid());
        Assert.assertTrue(podContext instanceof ContainerPlatformContextAspectApiDTO);
        context = (ContainerPlatformContextAspectApiDTO)podContext;
        Assert.assertNull(context.getNamespace());
        Assert.assertEquals(containerCluster.getDisplayName(), context.getContainerPlatformCluster());
    }
}
