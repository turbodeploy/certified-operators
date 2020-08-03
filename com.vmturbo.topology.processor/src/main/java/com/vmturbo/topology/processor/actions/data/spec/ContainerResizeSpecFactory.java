package com.vmturbo.topology.processor.actions.data.spec;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;

/**
 * Utility class to create a {@link DataRequirementSpec} representing the need to include the UUID
 * of the service (if any) being hosted by the container in the context data for
 * container resize actions. This is needed for the Cloud Foundry probe, and possibly others.
 */
public class ContainerResizeSpecFactory {

    /**
     * An interface for making remote calls to the Search service, to retrieve details about
     * entities involved in actions.
     */
    private final SearchServiceBlockingStub searchServiceRpc;

    /**
     * Convert topology processor's entity DTOs to entity DTOs used by SDK probes.
     * Needed to extract the UUID for a given entity, since these are not stored in the Repository.
     */
    private final TopologyToSdkEntityConverter topologyToSdkEntityConverter;

    /**
     * @param searchServiceRpc an interface for making remote calls to the Search service
     * @param topologyToSdkEntityConverter to extract the UUID for a given entity
     */
    public ContainerResizeSpecFactory(
            @Nonnull final SearchServiceBlockingStub searchServiceRpc,
            @Nonnull final TopologyToSdkEntityConverter topologyToSdkEntityConverter) {
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.topologyToSdkEntityConverter = Objects.requireNonNull(topologyToSdkEntityConverter);
    }

    /**
     * Create a {@link DataRequirementSpec} representing the need to include a list of host names
     * in the context data for storage provision actions.
     *
     * @return a {@link DataRequirementSpec}
     */
    public DataRequirementSpec getContainerResizeSpec() {
        return new DataRequirementSpecBuilder()
                .addMatchCriteria(actionInfo -> actionInfo.hasResize())
                .addMatchCriteria(actionInfo -> EntityType.CONTAINER.getNumber() ==
                        actionInfo.getResize().getTarget().getType())
                .addDataRequirement(SDKConstants.SERVICE_UUID, actionInfo ->
                        getServiceUuidForAction(actionInfo).orElse(""))
                .build();
    }

    /**
     * Get the service UUID (if any) for the container in this resize action
     *
     * @param actionInfo a container resize action
     * @return the service UUID (if any) for the container in this resize action
     */
    private Optional<String> getServiceUuidForAction(ActionInfo actionInfo) {
        // Extract the OID of the container being resized from the ActionInfo
        Optional<Long> containerId = Stream.of(actionInfo)
                .map(ActionInfo::getResize)
                .map(Resize::getTarget)
                .filter(ActionEntity::hasId)
                .map(ActionEntity::getId)
                .findFirst();
        if(containerId.isPresent()) {
            // Search for the service (indirectly) consuming from this container
            return getServicesConsumingFromContainer(containerId.get()).stream()
                    // Converting the entity to SDK format allows us to extract the UUID instead
                    // of the OID
                    .map(entity -> topologyToSdkEntityConverter.convertToEntityDTO(entity))
                    .map(EntityDTO::getId)
                    // If no service is found, that's okay -- not all containers have one
                    .findAny();
        }
        return Optional.empty();
    }

    /**
     * Helper method to get TopologyEntityDTOs for given search criteria.
     * This search is for services that are consuming indirectly from the container
     * whose ID is provided as a parameter. The search uses 2 hops because there will be an
     * application between the container and the service in the supply chain.
     *
     * @param containerId the ID of the container whose service(s) should be retrieved
     * @return a list of services (indirectly) consuming from the provided container
     */
    private List<TopologyEntityDTO> getServicesConsumingFromContainer(
            @Nonnull long containerId) {
        final SearchParameters searchParameter = SearchParameters.newBuilder()
            // start from container entity oid
            .setStartingFilter(SearchProtoUtil.idFilter(containerId))
            // traverse PRODUCES relationship (Container produces commodities that applications
            // and services consume)
            .addSearchFilter(SearchFilter.newBuilder()
                .setTraversalFilter(TraversalFilter.newBuilder()
                    .setTraversalDirection(TraversalDirection.PRODUCES)
                    .setStoppingCondition(StoppingCondition.newBuilder()
                        // Max of 2 hops - one to reach Application layer and another
                        // hop to get from Application to Service layer.
                        .setNumberHops(2).build()))
                .build())
            // find all services consuming from this entity
            .addSearchFilter(SearchFilter.newBuilder()
                .setPropertyFilter(SearchProtoUtil.entityTypeFilter(EntityType.SERVICE_VALUE)))
            .build();

        return SpecSearchUtil.searchTopologyEntityDTOs(searchParameter,
                searchServiceRpc);
    }

}
