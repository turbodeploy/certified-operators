package com.vmturbo.topology.processor.actions.data.spec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
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
 * Utility class to create a {@link DataRequirementSpec} representing the need to include attached
 * VM info in the context data for volume scale actions. This is needed for Azure probe.
 */
public class VolumeScaleSpecFactory {

    private static final String NON_EMPTY_REGEX = ".+";

    /**
     * An interface for making remote calls to the Search service, to retrieve details about
     * entities involved in actions.
     */
    private final SearchServiceBlockingStub searchServiceRpc;

    /**
     * VolumeScaleSpecFactory to create context data for volume scale action.
     *
     * @param searchServiceRpc an interface for making remote calls to the Search service
     */
    public VolumeScaleSpecFactory(@Nonnull final SearchServiceBlockingStub searchServiceRpc) {
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
    }

    /**
     * Create a {@link DataRequirementSpec} representing the need to include attached VM info
     * in the context data for volume scale actions.
     *
     * @return a {@link DataRequirementSpec}
     */
    public DataRequirementSpec getVolumeScaleSpec() {
        return new DataRequirementSpecBuilder()
                .addMatchCriteria(ActionInfo::hasScale)
                .addMatchCriteria(actionInfo -> EntityType.VIRTUAL_VOLUME.getNumber()
                        == actionInfo.getScale().getTarget().getType())
                .addMultiValueRequirement(this::getAttachedVMInfo)
                .build();
    }

    private List<ContextData> getAttachedVMInfo(ActionInfo actionInfo) {
        final long volumeId = actionInfo.getScale().getTarget().getId();
        TopologyEntityDTO vm = getVMsConsumingFromVolume(volumeId).stream().findAny().orElse(null);
        if (vm == null) {
            return Collections.emptyList();
        }
        final List<ContextData> res = new ArrayList<>();
        final ContextData vmNameContextData = ContextData.newBuilder()
                .setContextKey(SDKConstants.VM_NAME)
                .setContextValue(vm.getDisplayName())
                .build();
        final String vmResourceGroupName = vm.getEntityPropertyMapOrDefault(SDKConstants.RESOURCE_GROUP_NAME, "");
        final ContextData vmResourceGroupNameContextData = ContextData.newBuilder()
                .setContextKey(SDKConstants.RESOURCE_GROUP_NAME)
                .setContextValue(vmResourceGroupName)
                .build();
        res.add(vmNameContextData);
        res.add(vmResourceGroupNameContextData);
        return res;
    }

    private List<TopologyEntityDTO> getVMsConsumingFromVolume(@Nonnull long volumeId) {
        final SearchParameters searchParameter = SearchParameters.newBuilder()
                // start from volume oid
                .setStartingFilter(SearchProtoUtil.idFilter(volumeId))
                // traverse PRODUCES relationship (Volume produces commodities that VM consumes)
                .addSearchFilter(SearchFilter.newBuilder()
                        .setTraversalFilter(TraversalFilter.newBuilder()
                                .setTraversalDirection(TraversalDirection.PRODUCES)
                                .setStoppingCondition(StoppingCondition.newBuilder()
                                        // 1 hop to reach VM layer
                                        .setNumberHops(1).build()))
                        .build())
                // filter by VM type
                .addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(SearchProtoUtil.entityTypeFilter(EntityType.VIRTUAL_MACHINE_VALUE)))
                // ensure that the VM returned have a display name
                .addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(SearchProtoUtil.nameFilterRegex(NON_EMPTY_REGEX)))
                .build();

        return SpecSearchUtil.searchTopologyEntityDTOs(searchParameter, searchServiceRpc);
    }
}
