package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.GraphRequest;
import com.vmturbo.common.protobuf.search.Search.GraphResponse;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The connected entity mapper is responsible for creating base api dtos for connected entities for a
 * service entity. At the moment, this only extends to Cloud commitments where the region and business
 * account are added as connected entities.
 */
public class ConnectedEntityMapper {

    private static List<TraversalDirection> graphSearchList = ImmutableList.of(TraversalDirection.OWNED_BY, TraversalDirection.AGGREGATES);

    private static List<ConnectedEntityProperties> listConnectedEntitiesToCloudCommitment = ImmutableList.of(ConnectedEntityProperties.builder().entityType(EntityType.REGION).traversalDirection(TraversalDirection.AGGREGATED_BY).required(false).build(),
    ConnectedEntityProperties.builder().entityType(EntityType.BUSINESS_ACCOUNT).traversalDirection(TraversalDirection.OWNED_BY).required(true).build());

    private static Map<Integer, List<ConnectedEntityProperties>> connectedEntityPropertiesByEntityType =
            ImmutableMap.of(EntityType.CLOUD_COMMITMENT_VALUE, listConnectedEntitiesToCloudCommitment);

    private static final Logger logger = LogManager.getLogger();

    private final RepositoryServiceBlockingStub repositoryService;

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    private final Long realtimeTopologyContextId;

    /**
     * Constructor for the Connected Entity Mapper.
     *
     * @param repositoryService The repository service.
     * @param realtimeTopologyContextId The real time topology context id.
     * @param searchServiceBlockingStub The search service blocking stub.
     */
    public ConnectedEntityMapper(@Nonnull final RepositoryServiceBlockingStub repositoryService, @Nonnull final Long realtimeTopologyContextId,
            @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub) {
        this.repositoryService = repositoryService;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.searchServiceBlockingStub = searchServiceBlockingStub;
    }

    /**
     * Given an api partial entity, maps the connected entities and returns a set of base api dtos.
     *
     * @param entity The partial api entity.
     *
     * @return A set of base api dtos.
     */
    public Set<BaseApiDTO> mapConnectedEntities(ApiPartialEntity entity) {
        LinkedHashSet<BaseApiDTO> baseApiDTOS = new LinkedHashSet<>();
        long entityOid = entity.getOid();
        if (connectedEntityPropertiesByEntityType.keySet().contains(entity.getEntityType())) {
            List<ConnectedEntityProperties> connectedEntityProperties = connectedEntityPropertiesByEntityType.get(entity.getEntityType());
            for (ConnectedEntityProperties connectedEntityProperty : connectedEntityProperties) {
                if (graphSearchList.contains(connectedEntityProperty.traversalDirection())) {
                    List<BaseApiDTO> graphSearchBaseApiDTO = getGraphSearchBaseApiDTO(entityOid, connectedEntityProperty.traversalDirection(), connectedEntityProperty.entityType());
                    if (graphSearchBaseApiDTO.isEmpty()) {
                        if (connectedEntityProperty.required()) {
                            logger.error("Couldn't find connected entity {} for entity {} ",
                                    connectedEntityProperty.entityType(), entityOid);
                        } else {
                            logger.debug("Couldn't find connected entity {} for entity {} ",
                                    connectedEntityProperty.entityType(), entityOid);
                        }
                    } else {
                        baseApiDTOS.addAll(graphSearchBaseApiDTO);
                    }
                } else {
                    Optional<RelatedEntity> repositoryRetrievedEntity =
                            entity.getConnectedToList()
                                    .stream()
                                    .filter(s -> s.getEntityType() == connectedEntityProperty.entityType().getNumber())
                                    .findFirst();
                    if (!repositoryRetrievedEntity.isPresent()) {
                        logger.error(
                                "Connected entity {} not found in connected entities list of the cloud commitment {}",
                                connectedEntityProperty.entityType(), entity.getOid());
                    } else {
                        Optional<TopologyEntityDTO> regionEntity = getEntity(
                                repositoryRetrievedEntity.get().getOid(), connectedEntityProperty.entityType().getNumber());
                        if (!regionEntity.isPresent()) {
                            if (connectedEntityProperty.required()) {
                                logger.error(
                                        "Repository could not retrieve entity {} for the cloud commitment {}",
                                        connectedEntityProperty.entityType(), entity.getOid());
                            } else {
                                logger.debug(
                                        "Repository could not retrieve entity {} for the cloud commitment {}",
                                        connectedEntityProperty.entityType(), entity.getOid());
                            }
                        } else {
                            baseApiDTOS.add(toBaseApiDTO(regionEntity.get()));
                        }
                    }
                }
            }
        }
        return baseApiDTOS;
    }

    /**
     * Given a topology entity dto, maps the region and location to base api dtos.
     *
     * @param entity The partial api entity.
     *
     * @return A set of base api dtos.
     */
    public Set<BaseApiDTO> mapConnectedEntities(TopologyEntityDTO entity) {
        LinkedHashSet<BaseApiDTO> baseApiDTOS = new LinkedHashSet<>();
        long entityOid = entity.getOid();
        if (connectedEntityPropertiesByEntityType.keySet().contains(entity.getEntityType())) {
            List<ConnectedEntityProperties> connectedEntityProperties = connectedEntityPropertiesByEntityType.get(entity.getEntityType());
            for (ConnectedEntityProperties connectedEntityProperty : connectedEntityProperties) {
                if (graphSearchList.contains(connectedEntityProperty.traversalDirection())) {
                    List<BaseApiDTO> graphSearchBaseApiDTO = getGraphSearchBaseApiDTO(entityOid, connectedEntityProperty.traversalDirection(), connectedEntityProperty.entityType());
                    if (graphSearchBaseApiDTO.isEmpty()) {
                        if (connectedEntityProperty.required()) {
                            logger.error("Couldn't find connected entity {} for entity {} ",
                                    connectedEntityProperty.entityType(), entityOid);
                        } else {
                            logger.debug("Couldn't find connected entity {} for entity {} ",
                                    connectedEntityProperty.entityType(), entityOid);
                        }
                    } else {
                        baseApiDTOS.addAll(graphSearchBaseApiDTO);
                    }
                } else {
                    Optional<ConnectedEntity> repositoryRetrievedEntity =
                            entity.getConnectedEntityListList().stream().filter(s -> s.getConnectedEntityType()
                                    == connectedEntityProperty.entityType().getNumber()).findFirst();
                    if (!repositoryRetrievedEntity.isPresent()) {
                        logger.error(
                                "Connected entity {} not found in connected entities list of the cloud commitment {}",
                                connectedEntityProperty.entityType(), entity.getOid());
                    } else {
                        Optional<TopologyEntityDTO> regionEntity = getEntity(
                                repositoryRetrievedEntity.get().getConnectedEntityId(), connectedEntityProperty.entityType().getNumber());
                        if (!regionEntity.isPresent()) {
                            if (connectedEntityProperty.required()) {
                                logger.error(
                                        "Repository could not retrieve entity {} for the cloud commitment {}",
                                        connectedEntityProperty.entityType(), entity.getOid());
                            } else {
                                logger.debug("Repository could not retrieve entity {} for the cloud commitment {}",
                                        connectedEntityProperty.entityType(), entity.getOid());
                            }
                        } else {
                            baseApiDTOS.add(toBaseApiDTO(regionEntity.get()));
                        }
                    }
                }
            }
        }
        return baseApiDTOS;
    }

    /**
     * Create base {@link ServiceEntityApiDTO} with uuid, display name, class name fields from
     * {@link RelatedEntity}.
     *
     * @param topologyEntityDTO the {@link RelatedEntity}
     * @return the {@link ServiceEntityApiDTO}
     */
    private static BaseApiDTO toBaseApiDTO(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        final BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid(String.valueOf(topologyEntityDTO.getOid()));
        baseApiDTO.setDisplayName(topologyEntityDTO.getDisplayName());
        baseApiDTO.setClassName(ApiEntityType.fromType(topologyEntityDTO.getEntityType()).apiStr());
        return baseApiDTO;
    }

    /**
     * Create base {@link ServiceEntityApiDTO} with uuid, display name, class name fields from
     * {@link MinimalEntity}.
     *
     * @param minimalEntity the {@link MinimalEntity}
     * @return the {@link ServiceEntityApiDTO}
     */
    private static BaseApiDTO toBaseApiDTO(@Nonnull final MinimalEntity minimalEntity) {
        final BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid(String.valueOf(minimalEntity.getOid()));
        baseApiDTO.setDisplayName(minimalEntity.getDisplayName());
        baseApiDTO.setClassName(ApiEntityType.fromType(minimalEntity.getEntityType()).apiStr());
        return baseApiDTO;
    }

    /**
     * Given an entity oid, does a graph search and returns the entity given a particular traversal direction for the
     * entity.
     *
     * @param entityOid The entity oid.
     * @param traversalDirection The firection in which to traverse to find the entity.
     * @param entityType The entity Type.
     * @return A list of size 1 in which the first element is the business account.
     */
    private List<BaseApiDTO> getGraphSearchBaseApiDTO(Long entityOid, TraversalDirection traversalDirection, EntityType entityType) {
        String entityName = ApiEntityType.fromSdkTypeToEntityTypeString(entityType.getNumber());
        List<BaseApiDTO> baseApiDTOList = new ArrayList<>();
        GraphRequest request = GraphRequest.newBuilder().addOids(entityOid).putNodes(entityName,
                SearchProtoUtil.node(Type.MINIMAL, traversalDirection, entityType).build()).build();
        GraphResponse response = searchServiceBlockingStub.graphSearch(request);
        SearchProtoUtil.getMapMinimal(response.getNodesOrThrow(entityName)).forEach((oid, accounts) -> {
            if (accounts != null && !accounts.isEmpty()) {
                baseApiDTOList.add(toBaseApiDTO(accounts.get(0)));
            }
        });
        return baseApiDTOList;
    }

    private Optional<TopologyEntityDTO> getEntity(Long entityOid, int entityType) {
        RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityOids(entityOid)
                .setTopologyContextId(realtimeTopologyContextId)
                .addEntityType(entityType)
                .setTopologyType(TopologyType.SOURCE)
                .build();
        final Iterator<PartialEntityBatch> iterator = repositoryService.retrieveTopologyEntities(
                request);
        while (iterator.hasNext()) {
            final PartialEntityBatch batch = iterator.next();
            for (PartialEntity partialEntity : batch.getEntitiesList()) {
                final TopologyEntityDTO entity = partialEntity.getFullEntity();
                return Optional.ofNullable(entity);
            }
        }
        return Optional.empty();
    }

    /**
     * An immutable class to represent connected entity properties.
     */
    @Immutable
    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    public interface ConnectedEntityProperties {

        /**
         * The entity type of the connected entity.
         *
         * @return The entity type.
         */
        EntityType entityType();

        /**
         * The traversal direction for the connected entity.
         *
         * @return The traversal direction.
         */
        TraversalDirection traversalDirection();

        /**
         * Returns a builder of the ConnectedEntityProperties class.
         *
         * @return The builder.
         */
        static ConnectedEntityProperties.Builder builder() {
            return new ConnectedEntityProperties.Builder();
        }

        /**
         * If the connected entity is required and the absence of which indicates an error.
         *
         * @return True if required.
         */
        boolean required();

        /**
         * Static inner class for extending generated or yet to be generated builder.
         */
        class Builder extends ImmutableConnectedEntityProperties.Builder {}
    }
}
