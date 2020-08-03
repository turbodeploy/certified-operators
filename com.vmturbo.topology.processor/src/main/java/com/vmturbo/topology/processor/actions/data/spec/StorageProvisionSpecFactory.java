package com.vmturbo.topology.processor.actions.data.spec;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility class to create a {@link DataRequirementSpec} representing the need to include a list of
 * host names in the context data for storage provision actions. Specifically, the display name of
 * each host consuming from the entity-to-clone storage will be stored in a ContextData element.
 */
public class StorageProvisionSpecFactory {

    private static final String NON_EMPTY_REGEX = ".+";

    /**
     * An interface for making remote calls to the Search service, to retrieve details about
     * entities involved in actions.
     */
    private final SearchServiceBlockingStub searchServiceRpc;

    /**
     * @param searchServiceRpc an interface for making remote calls to the Search service
     */
    public StorageProvisionSpecFactory(@Nonnull final SearchServiceBlockingStub searchServiceRpc) {
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
    }

    /**
     * Create a {@link DataRequirementSpec} representing the need to include a list of host names
     * in the context data for storage provision actions.
     *
     * @return a {@link DataRequirementSpec}
     */
    public DataRequirementSpec getStorageProvisionSpec() {
        return new DataRequirementSpecBuilder()
                .addMatchCriteria(actionInfo -> actionInfo.hasProvision())
                .addMatchCriteria(actionInfo -> EntityType.STORAGE.getNumber() ==
                        actionInfo.getProvision().getEntityToClone().getType())
                .addMultiValueRequirement(this::getHostNamesRelatedToStorage)
                .build();
    }

    private List<ContextData> getHostNamesRelatedToStorage(ActionInfo actionInfo) {
        Optional<Long> storageId = Stream.of(actionInfo)
                .map(ActionInfo::getProvision)
                .map(Provision::getEntityToClone)
                .filter(ActionEntity::hasId)
                .map(ActionEntity::getId)
                .findFirst();
        // Convert the list of hosts into a list of ContextData containing the host names
        if(storageId.isPresent()) {
            return getHostsConsumingFromStorageEntity(storageId.get()).stream()
                    .map(TopologyEntityDTO::getDisplayName)
                    .map(displayName -> ContextData.newBuilder()
                            .setContextKey(SDKConstants.HOST_NAMES)
                            .setContextValue(displayName)
                            .build())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * Helper method to get TopologyEntityDTOs for given search criteria.
     */
    private List<TopologyEntityDTO> getHostsConsumingFromStorageEntity(long storageId) {
        final SearchParameters searchParameter = SearchParameters.newBuilder()
            // start from storage entity oid
            .setStartingFilter(SearchProtoUtil.idFilter(storageId))
            // traverse PRODUCES relationship (Storage produces a storage access commodity that
            // PMs consume)
            .addSearchFilter(SearchFilter.newBuilder()
                .setTraversalFilter(TraversalFilter.newBuilder()
                    .setTraversalDirection(TraversalDirection.PRODUCES)
                    .setStoppingCondition(StoppingCondition.newBuilder()
                        .setNumberHops(1).build()))
                .build())
            // find all hosts consuming from this storage entity
            .addSearchFilter(SearchFilter.newBuilder()
                .setPropertyFilter(SearchProtoUtil.entityTypeFilter(EntityType.PHYSICAL_MACHINE_VALUE)))
            // ensure that all hosts returned have a display name
            .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(SearchProtoUtil.nameFilterRegex(NON_EMPTY_REGEX)))
            .build();

        return SpecSearchUtil.searchTopologyEntityDTOs(
            searchParameter,
            searchServiceRpc);
    }

}
