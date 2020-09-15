package com.vmturbo.action.orchestrator.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Determine when expansion is needed. Also determines how to expand.
 */
public class InvolvedEntitiesExpander {

    /**
     * These are the entities that need to be retrieved underneath BusinessApp, BusinessTxn, and
     * Service.
     * <pre>
     * BApp -> BTxn -> Service -> AppComp -> Node
     *                                       VirtualMachine  --> VDC ----> Host  --------
     *                       DatabaseServer   \    \   \             ^                \
     *                                          \    \   \___________/                v
     *                                           \    -----> Volume   ------------->  Storage
     *                                            \                                   ^
     *                                             ----------------------------------/
     * </pre>
     */
    public static final List<String> PROPAGATED_ARM_ENTITY_TYPES = Arrays.asList(
        ApiEntityType.APPLICATION_COMPONENT.apiStr(),
        ApiEntityType.CONTAINER.apiStr(),
        ApiEntityType.CONTAINER_POD.apiStr(),
        ApiEntityType.VIRTUAL_MACHINE.apiStr(),
        ApiEntityType.DATABASE_SERVER.apiStr(),
        ApiEntityType.VIRTUAL_VOLUME.apiStr(),
        ApiEntityType.STORAGE.apiStr(),
        ApiEntityType.PHYSICAL_MACHINE.apiStr());

    private static final Set<String> ENTITY_TYPES_BELOW_ARM = ImmutableSet.<String>builder()
        .addAll(PROPAGATED_ARM_ENTITY_TYPES)
        .add(ApiEntityType.BUSINESS_APPLICATION.apiStr())
        .add(ApiEntityType.BUSINESS_TRANSACTION.apiStr())
        .add(ApiEntityType.SERVICE.apiStr())
        .build();

    private static final Set<Integer> ARM_ENTITY_TYPE = ImmutableSet.of(
        EntityType.BUSINESS_APPLICATION.getValue(),
        EntityType.BUSINESS_TRANSACTION.getValue(),
        EntityType.SERVICE.getValue());

    private final Map<Integer, Set<Long>> expandedEntitiesPerARMEntityType = new HashMap<>();

    private static Logger logger = LogManager.getLogger();

    private final RepositoryServiceBlockingStub repositoryService;

    private final SupplyChainServiceBlockingStub supplyChainService;

    /**
     * Creates an instance that uses the provided services for calculating expansion.
     *
     * @param repositoryService service used to determine if expansion is needed.
     * @param supplyChainService service used to determine the oids after expansion.
     */
    public InvolvedEntitiesExpander(
            @Nonnull final RepositoryServiceBlockingStub repositoryService,
            @Nonnull final SupplyChainServiceBlockingStub supplyChainService) {
        this.repositoryService = repositoryService;
        this.supplyChainService = supplyChainService;
    }

    /**
     * Check if an entity's actions should be propagated to the required ARM entity type.
     *
     * @param involvedEntityId the involved entity ID.
     * @param desiredEntityTypes the ARM entity types in the query.
     * @return true if this entity's actions should be propagated to the required ARM entity types.
     */
    public boolean isBelowARMEntityType(long involvedEntityId, Set<Integer> desiredEntityTypes) {
        return desiredEntityTypes.stream().anyMatch(entityType -> isARMEntityType(entityType)
                && expandedEntitiesPerARMEntityType.get(entityType).contains(involvedEntityId));
    }

    /**
     * Check if the given entity type is an ARM entity type.
     *
     * @param entityType the entity type.
     * @return true if this is an ARM entity type.
     */
    public boolean isARMEntityType(int entityType) {
        return ARM_ENTITY_TYPE.contains(entityType);
    }

    /**
     * Determines the filter needed for the input involvedEntities. If involvedEntities contains
     * at least one non-ARM entity, then they will not be expanded. If they are all ARM entities,
     * they will be expanded to include the entities below it in the supply chain.
     *
     * @param involvedEntities the entities to be expanded.
     * @return the filter needed for the input involvedEntities.
     */
    public InvolvedEntitiesFilter expandInvolvedEntitiesFilter(
            @Nonnull final Collection<Long> involvedEntities) {
        if (areAllARMEntities(involvedEntities)) {
            return new InvolvedEntitiesFilter(
                expandARMEntities(involvedEntities),
                InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
        }
        return new InvolvedEntitiesFilter(
            new HashSet<>(involvedEntities),
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
    }

    /**
     * Queries repositoryService to see if we need to expand these entities. If the entire list
     * contains ARM entities, then the involved entities will need to be expanded.
     *
     * @param involvedEntities the entities to search if we need expansion.
     * @return true if we need expansion.
     */
    private boolean areAllARMEntities(@Nonnull final Collection<Long> involvedEntities) {
        if (involvedEntities.isEmpty()) {
            return false;
        }

        // isPresent indicates not all arm, so we need to invert the result from isPresent
        return !Streams.stream(
            // get the entities from the repository
            repositoryService.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                    .addAllEntityOids(involvedEntities)
                    .setReturnType(Type.MINIMAL)
                    .build()))
            // entities are returned in batches
            // so flatten the batches into a single stream
            .flatMap(partialEntityBatch -> partialEntityBatch.getEntitiesList().stream())
            // double check it's minimal
            .filter(PartialEntity::hasMinimal)
            // extract the minimal entity from the partial entity
            .map(PartialEntity::getMinimal)
            // get the type from the minimal entity
            .map(MinimalEntity::getEntityType)
            // look for any entity type that is not arm (BApp/BTxn/Service)
            // anyMatch short circuits
            .anyMatch(entityType -> !ARM_ENTITY_TYPE.contains(entityType));
    }

    /**
     * Expand all ARM entities by type and save the mapping for later use.
     */
    public void expandAllARMEntities() {
        expandedEntitiesPerARMEntityType.clear();
        ARM_ENTITY_TYPE.forEach(armEntityType -> {
            Set<Long> armEntities = RepositoryDTOUtil.topologyEntityStream(
                    repositoryService.retrieveTopologyEntities(
                            RetrieveTopologyEntitiesRequest.newBuilder()
                                    .addEntityType(armEntityType)
                                    .setReturnType(Type.MINIMAL)
                                    .build()))
                    .map(partialEntity -> partialEntity.getMinimal().getOid())
                    .collect(Collectors.toSet());
            expandedEntitiesPerARMEntityType.put(armEntityType, expandARMEntities(armEntities));
        });
    }

    /**
     * Expands the involved entities to include the entities below. This allows use to link actions
     * like move VM from PM1 to PM2 when a business app uses the VM.
     *
     * @param involvedEntities the entities to expand.
     * @return the expanded entities.
     */
    @Nonnull
    private Set<Long> expandARMEntities(Collection<Long> involvedEntities) {
        GetSupplyChainRequest supplyChainRequest = GetSupplyChainRequest.newBuilder()
            .setScope(SupplyChainScope.newBuilder()
                .addAllStartingEntityOid(involvedEntities)
                .addAllEntityTypesToInclude(ENTITY_TYPES_BELOW_ARM)
                .build())
            .build();
        GetSupplyChainResponse supplyChainResponse = supplyChainService.getSupplyChain(supplyChainRequest);

        // When supplyChainService is unable to fill in the supply chain, the response will not
        // contain a supply chain. We do not want to stop the request from processing, but we need
        // logging so that we can track when this happens.
        if (!supplyChainResponse.hasSupplyChain()) {
            logger.error("Unable to expand the entities using request: " + supplyChainRequest.toString());
            return Sets.newHashSet(involvedEntities);
        }

        // extract the oids from the supply chain response
        return supplyChainResponse.getSupplyChain().getSupplyChainNodesList().stream()
            // supply chain nodes are organized into maps of states to members of that state
            .map(SupplyChainNode::getMembersByStateMap)
            // we are not interested in the states, so we ignore the key of the map
            .map(Map::values)
            // we have a stream of List of member lists, so we flatten to stream of member list
            .flatMap(Collection::stream)
            // extract the oids from each member list
            .map(MemberList::getMemberOidsList)
            // we have a stream of List of longs, collapse it to a single stream of longs
            .flatMap(List::stream)
            // make it a set to get rid of duplicates
            .collect(Collectors.toSet());
    }

    /**
     * Structure for holding the result of expansion by
     * {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
     */
    public static class InvolvedEntitiesFilter {

        private final Set<Long> entities;
        private final InvolvedEntityCalculation calculationType;

        /**
         * Constructs the filter with the provided entities and calculation type.
         *
         * @param entities the expanded entities.
         * @param calculationType the calculation type for filtering involved entities.
         */
        public InvolvedEntitiesFilter(
                final @Nonnull Set<Long> entities,
                final @Nonnull InvolvedEntityCalculation calculationType) {
            this.entities = entities;
            this.calculationType = calculationType;
        }

        /**
         * Returns the entities expanded by
         * {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         *
         * @return the entities expanded by
         *         {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         */
        @Nonnull
        public Set<Long> getEntities() {
            return entities;
        }

        /**
         * Returns the calculation type determined by
         * {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         *
         * @return the calculation type determined by
         *         {@link InvolvedEntitiesExpander#expandInvolvedEntitiesFilter(Collection)}.
         */
        @Nonnull
        public InvolvedEntityCalculation getCalculationType() {
            return calculationType;
        }
    }
}
