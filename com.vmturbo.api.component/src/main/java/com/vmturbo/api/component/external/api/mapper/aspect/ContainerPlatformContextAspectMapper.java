package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.ContainerPlatformContextAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The container platform context aspect mapper is used to map namespace and container cluster
 * info for container platform entities to {@link ContainerPlatformContextAspectApiDTO}.
 */
public class ContainerPlatformContextAspectMapper extends AbstractAspectMapper {

    private final ContainerPlatformContextMapper containerPlatformContextMapper;

    /**
     * Constructor for the ContainerPlatformContextAspectMapper.
     *
     * @param supplyChainRpcService Supply chain search service
     * @param repositoryApi  Repository access to fetch entities.
     * @param realtimeTopologyContextId The real time topology context id.
     *                                 Note: This only permits the lookup of aspects on the
     *                                 realtime topology and not plan entities.
     */
    public ContainerPlatformContextAspectMapper(@Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                          @Nonnull final RepositoryApi repositoryApi,
                                          @Nonnull final Long realtimeTopologyContextId) {
        containerPlatformContextMapper = new ContainerPlatformContextMapper(supplyChainRpcService,
                                                            repositoryApi, realtimeTopologyContextId);
    }

    @Override
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatchPartial(@Nonnull List<ApiPartialEntity> entities)
            throws InterruptedException, ConversionException {
        Map<Long, EntityAspect> aspectMap = containerPlatformContextMapper.bulkMapContainerPlatformContext(entities);
        return Optional.of(aspectMap);
    }

    /**
     * Map a list of TopologyEntityDTO objects belonging to Container Platform to the corresponding
     * ContainerPlatformContextAspectApiDTO objects.
     *
     * @param entities a list of TopologyEntityDTO objects belonging to Container Platform.
     *                 Each identified by unique oid.
     * @return A map of oid -> ContainerPlatformContextAspectApiDTO objects.
     */
    @Nonnull
    @Override
    public Optional<Map<Long, EntityAspect>> mapEntityToAspectBatch(@Nonnull final List<TopologyEntityDTO> entities) {
        Map<Long, EntityAspect> aspectMap = containerPlatformContextMapper.getContainerPlatformContext(entities);
        return Optional.of(aspectMap);
    }

    /**
     * Map a single {@link TopologyEntityDTO} into one entity aspect object.
     *
     * @param entity the {@link TopologyEntityDTO} to get aspect for
     * @return the entity aspect for the given entity, or null if no aspect for this entity
     */
    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        Optional<Map<Long, EntityAspect>> result = mapEntityToAspectBatch(Arrays.asList(entity));

        if (!result.isPresent()) {
            return result.get().get(entity.getOid());

        }
        return null;
    }

    /**
     * Returns the aspect name that can be used for filtering.
     *
     * @return the name of the aspect
     */
    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.CONTAINER_PLATFORM_CONTEXT;
    }


    /**
     * Convenience class to obtain container platform context information for entities
     * belonging to cloud native environments such as Kubernetes.
     * The context information consists of the namespace and container cluster that the entity belongs
     * and is obtained by traversing the supply chain to find namespace and cluster entities
     * that the entity is connected to. The context information is then
     * represented as {@link ContainerPlatformContextAspectApiDTO} for api calls.
     *
     */
     @VisibleForTesting
     static class ContainerPlatformContextMapper {
        private final Logger logger = LogManager.getLogger();
        private final long realtimeTopologyContextId;
        private final SupplyChainServiceBlockingStub supplyChainRpcService;
        private final RepositoryApi repositoryApi;

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
         * @param repositoryApi To fetch entities.
         * @param realtimeTopologyContextId The real time topology context id.
         *                                  Note: This only permits the lookup of aspects on the
         *                                    realtime topology and not plan entities.
         */
        ContainerPlatformContextMapper(@Nonnull final SupplyChainServiceBlockingStub supplyChainRpcService,
                                              @Nonnull final RepositoryApi repositoryApi,
                                              @Nonnull final Long realtimeTopologyContextId) {
            this.supplyChainRpcService = supplyChainRpcService;
            this.repositoryApi = repositoryApi;
            this.realtimeTopologyContextId = realtimeTopologyContextId;
        }

        /**
         * Return the {@link ContainerPlatformContextAspectApiDTO} containing the namespace
         * and container platform cluster for the given set of topology entities.
         *
         * @param entities Collection for {@link ApiPartialEntity}'s
         * @return map containing the entity OID and its corresponding container platform aspect
         */
         Map<Long, EntityAspect> bulkMapContainerPlatformContext(Collection<ApiPartialEntity> entities) {
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
                        final Set<Long> members = supplyChainResponse.getSupplyChain()
                            .getSupplyChainNodesList()
                            .stream()
                            .filter(node -> CLOUD_NATIVE_ENTITY_CONNECTIONS.contains(node.getEntityType()))
                            .map(SupplyChainNode::getMembersByStateMap)
                            .map(Map::values)
                            .flatMap(memberList -> memberList.stream()
                                    .map(MemberList::getMemberOidsList)
                                    .flatMap(List::stream))
                            .collect(Collectors.toSet());

                        if (!members.isEmpty()) {
                            visited.addAll(members);
                            connectedEntityIds.put(oid, members);
                        }
                    });
            } catch (StatusRuntimeException e) {
                logger.error("Failed to retrieve cloud native context entities from supply chain", e);
            }

            if (!connectedEntityIds.isEmpty()) {
                // Second repository rpc call to get the name for each of the unique connection entity
                Map<Long, MinimalEntity> repositoryEntities = getRepositoryEntities(visited);

                connectedEntityIds.forEach((eId, connections) -> {
                    final ContainerPlatformContextAspectApiDTO aspect = new ContainerPlatformContextAspectApiDTO();
                    for (Long connectionId : connections) {
                        if (!repositoryEntities.containsKey(connectionId)) {
                            logger.warn("Missing repository info for {}", connectionId);
                            continue;
                        }
                        MinimalEntity minimalEntity = repositoryEntities.get(connectionId);
                        if (minimalEntity.hasDisplayName() && minimalEntity.hasEntityType()) {
                            if (minimalEntity.getEntityType() == EntityType.NAMESPACE_VALUE) {
                                aspect.setNamespace(minimalEntity.getDisplayName());
                            } else if (minimalEntity.getEntityType() == EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE) {
                                aspect.setContainerPlatformCluster(minimalEntity.getDisplayName());
                            }
                        }
                    }

                    connectedEntities.put(eId, aspect);
                });
            }

            return connectedEntities;
        }

        /**
         * Retrieve the entities from the repository for the given list of OIDs.
         * @param entityOids list of entity OIDs
         * @return Map containing the entity OID and its {@link BaseApiDTO}
         */
        private Map<Long, MinimalEntity> getRepositoryEntities(Set<Long> entityOids) {
            Map<Long, MinimalEntity> minimalEntityMap = new HashMap<>(entityOids.size());
            if (entityOids.isEmpty()) {
                return minimalEntityMap;
            }

            try {
                repositoryApi.entitiesRequest(entityOids).getMinimalEntities().forEach(minimalEntity -> {
                    minimalEntityMap.put(minimalEntity.getOid(), minimalEntity);
                });
            } catch (StatusRuntimeException e) {
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
         Map<Long, EntityAspect> getContainerPlatformContext( Collection<TopologyEntityDTO> entities) {
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
}
