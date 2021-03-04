package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.ContainerPlatformContextAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Convenience class to obtain container platform context information for entities
 * belonging to cloud native environments such as Kubernetes.
 * The context information consists of the namespace and container cluster that the entity belongs
 * and is obtained by traversing the supply chain to find namespace and cluster entities
 * that the entity is connected to. The context information is then
 * represented as {@link ContainerPlatformContextAspectApiDTO} for api calls.
 *
 */
public class ContainerPlatformContextMapper {
    private final Logger logger = LogManager.getLogger();
    private final long realtimeTopologyContextId;
    private final SupplyChainServiceBlockingStub supplyChainRpcService;
    private final RepositoryServiceBlockingStub repositoryRpcService;

    private static final Set<Integer> CLOUD_NATIVE_ENTITY_CONNECTIONS
            = ImmutableSet.of(EntityType.NAMESPACE_VALUE, EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE);

    private static final Map<Integer, Set<Integer>> ENTITY_TYPE_TO_CONNECTIONS
            = ImmutableMap.<Integer, Set<Integer>>builder()
            .put(EntityType.CONTAINER_VALUE,           CLOUD_NATIVE_ENTITY_CONNECTIONS)
            .put(EntityType.CONTAINER_POD_VALUE,       CLOUD_NATIVE_ENTITY_CONNECTIONS)
            .put(EntityType.CONTAINER_SPEC_VALUE,      CLOUD_NATIVE_ENTITY_CONNECTIONS)
            .put(EntityType.WORKLOAD_CONTROLLER_VALUE, CLOUD_NATIVE_ENTITY_CONNECTIONS)
            .put(EntityType.SERVICE_VALUE,             CLOUD_NATIVE_ENTITY_CONNECTIONS)
            //VM's don't actually belong to a specific namespace so only fetch cluster name
            .put(EntityType.VIRTUAL_MACHINE_VALUE,      ImmutableSet.of(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE))
            .build();

    /**
     * Constructor for the ContainerPlatformContextMapper.
     *
     * @param supplyChainRpcService Supply chain search service
     * @param repositoryRpcService  Repository search service
     * @param realtimeTopologyContextId The real time topology context id.
     *                                  Note: This only permits the lookup of aspects on the
     *                                    realtime topology and not plan entities.
     */
    public ContainerPlatformContextMapper(@Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                          @Nonnull final RepositoryServiceBlockingStub repositoryRpcService,
                                          @Nonnull final Long realtimeTopologyContextId) {
        this.supplyChainRpcService = supplyChainRpcService;
        this.repositoryRpcService = repositoryRpcService;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Return the {@link ContainerPlatformContextAspectApiDTO} containing the namespace
     * and container platform cluster for the given set of topology entities.
     *
     * @param entities Collection for {@link ApiPartialEntity}'s
     * @return map containing the entity OID and its corresponding container platform aspect
     */
    public Map<Long, EntityAspect> bulkMapContainerPlatformContext(Collection<ApiPartialEntity> entities) {
        Map<Long, EntityAspect> entityAspectMap = new HashMap<>();

        // Request to the supply chain search service to find the cloud native connections
        final GetMultiSupplyChainsRequest.Builder requestBuilder =
                GetMultiSupplyChainsRequest.newBuilder()
                        .setContextId(realtimeTopologyContextId);

        entities.stream()
                .filter( entity -> ENTITY_TYPE_TO_CONNECTIONS.containsKey(entity.getEntityType()))
                .forEach( entity -> {
                    long entityOid = entity.getOid();
                    requestBuilder.addSeeds(
                            SupplyChainSeed.newBuilder()
                                    .setSeedOid(entityOid)
                                    .setScope(SupplyChainScope.newBuilder()
                                                    .addStartingEntityOid(entityOid)
                                                    .addAllEntityTypesToInclude(
                                                            ENTITY_TYPE_TO_CONNECTIONS.get(entity.getEntityType()))
                                    ));

                });

        if (requestBuilder.getSeedsCount() == 0) {
            return entityAspectMap;
        }

        return getCloudNativeConnectedEntities(requestBuilder);
    }

    /**
     * Search the supply chain for OIDs of the connected entities
     * and then retrieve the corresponding entities from the repository.
     *
     * @param requestBuilder GetMultiSupplyChainsRequest
     * @return map containing the entity OID and its corresponding container platform aspect
     */
    private Map<Long, EntityAspect> getCloudNativeConnectedEntities(GetMultiSupplyChainsRequest.Builder requestBuilder) {
        Map<Long, EntityAspect> connectedEntities = new HashMap<>();

        // First search the supply chain to get the connected namespace and cluster for each entity.
        // set of the OIDs for each unique connection entity
        Set<Long> visited = new HashSet<>();

        Map<Long, Set<Long>> connectedEntityIds = new HashMap<>();
        try {
            supplyChainRpcService.getMultiSupplyChains(requestBuilder.build())
                    .forEachRemaining(supplyChainResponse -> {
                        final long oid = supplyChainResponse.getSeedOid();
                        HashSet<Long> members = new HashSet<>();
                        supplyChainResponse.getSupplyChain()
                                .getSupplyChainNodesList()
                                .stream()
                                .filter(node -> CLOUD_NATIVE_ENTITY_CONNECTIONS.contains(node.getEntityType()))
                                .map(SupplyChainNode::getMembersByStateMap)
                                .map(Map::values)
                                .flatMap(memberList -> memberList.stream()
                                        .map(MemberList::getMemberOidsList)
                                        .flatMap(List::stream))
                                .forEach(memberId -> {
                                    visited.add(memberId);
                                    members.add(memberId);
                                });
                        connectedEntityIds.put(oid, members);
                    });

        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve cloud native context entities from supply chain", e);
        }

        // Second repository rpc call to get the name for each of the unique connection entity
        Map<Long, MinimalEntity> repositoryEntities = getRepositoryEntities(visited);

        connectedEntityIds.entrySet().stream().forEach(e -> {
            final ContainerPlatformContextAspectApiDTO aspect = new ContainerPlatformContextAspectApiDTO();
            for (long oid : e.getValue()) {
                if (!repositoryEntities.containsKey(oid))  {
                    logger.warn("Missing repository info for {}", oid);
                    continue;
                }
                MinimalEntity minimalEntity = repositoryEntities.get(oid);
                if (minimalEntity.hasDisplayName() && minimalEntity.hasEntityType()) {
                    if (minimalEntity.getEntityType()
                            == EntityType.NAMESPACE_VALUE) {
                        aspect.setNamespace(minimalEntity.getDisplayName());
                    } else if (minimalEntity.getEntityType()
                            == EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE) {
                        aspect.setContainerPlatformCluster(minimalEntity.getDisplayName());
                    }
                }
            }

            connectedEntities.put(e.getKey(), aspect);
        });

        return connectedEntities;
    }

    /**
     * Retrieve the entities from the repository for the given list of OIDs.
     * @param entityOids list of entity OIDs
     * @return Map containing the entity OID and its {@link BaseApiDTO}
     */
    private Map<Long, MinimalEntity> getRepositoryEntities(Collection<Long> entityOids) {
        Map<Long, MinimalEntity> minimalEntityMap = new HashMap<>();

        // Request to the repository service
        final RetrieveTopologyEntitiesRequest.Builder requestBuilder =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId);

        entityOids.stream().forEach(entityOid -> {
            requestBuilder.addEntityOids(entityOid)
                    .setTopologyContextId(realtimeTopologyContextId)
                    .setTopologyType(TopologyType.SOURCE)
                    .setReturnType(Type.MINIMAL);
        });
        try {
            final Iterator<PartialEntityBatch> iterator
                    = repositoryRpcService.retrieveTopologyEntities(requestBuilder.build());
            while (iterator.hasNext()) {
                final PartialEntityBatch batch = iterator.next();
                for (PartialEntity partialEntity : batch.getEntitiesList()) {
                    final MinimalEntity minimalEntity = partialEntity.getMinimal();
                    minimalEntityMap.put(minimalEntity.getOid(), minimalEntity);
                }
            }
        } catch(StatusRuntimeException e) {
            logger.error("Failed to retrieve cloud native context entities from repository", e);
        }

        return minimalEntityMap;
    }

    /**
     * Return the {@link ContainerPlatformContextAspectApiDTO} containing the namespace
     * and container platform cluster for the given set of topology entities.
     *
     * @param entities collection of {@link TopologyEntityDTO}
     * @return map containing the entity OID and its corresponding container platform aspect
     */
    public Map<Long, EntityAspect> getContainerPlatformContext( Collection<TopologyEntityDTO> entities) {
        Map<Long, EntityAspect> entityAspectMap = new HashMap<>();

        // Request to the supply chain search service to find the cloud native connections
        final GetMultiSupplyChainsRequest.Builder requestBuilder =
                GetMultiSupplyChainsRequest.newBuilder()
                        .setContextId(realtimeTopologyContextId);

        entities.stream()
                .filter( entity -> ENTITY_TYPE_TO_CONNECTIONS.containsKey(entity.getEntityType()))
                .forEach( entity -> {
                    Long entityOid = entity.getOid();
                    requestBuilder.addSeeds(
                            SupplyChainSeed.newBuilder()
                                    .setSeedOid(entityOid)
                                    .setScope(SupplyChainScope.newBuilder()
                                            .addStartingEntityOid(entityOid)
                                            .addAllEntityTypesToInclude(
                                                    ENTITY_TYPE_TO_CONNECTIONS.get(entity.getEntityType()))
                                    ));

                });

        if (requestBuilder.getSeedsCount() == 0) {
            return entityAspectMap;
        }

        return getCloudNativeConnectedEntities(requestBuilder);
    }
}
