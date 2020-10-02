package com.vmturbo.topology.processor.actions.data.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;

/**
 * Tracks data requirements for handling action execution special cases (i.e. complex actions)
 */
public class ActionDataManager {

    /**
     * A list of {@link DataRequirementSpec} used to identify and inject data into actions
     * Each spec determines both if an action meets its criteria (meaning the additional data is
     * required) and specifies how to inject the additional data into the action.
     */
    private final List<DataRequirementSpec> allDataRequirements = new ArrayList<>();

    /**
     * Initialize the action data manager, and create the default list of data requirement specs
     *
     * @param searchServiceRpc an interface for making remote calls to the Search service
     * @param topologyToSdkEntityConverter
     */
    public ActionDataManager(@Nonnull final SearchServiceBlockingStub searchServiceRpc,
                             @Nonnull final TopologyToSdkEntityConverter topologyToSdkEntityConverter)
    {
        Objects.requireNonNull(searchServiceRpc);
        Objects.requireNonNull(topologyToSdkEntityConverter);

        // Create a spec for container resize
        ContainerResizeSpecFactory containerResizeSpecFactory =
                new ContainerResizeSpecFactory(searchServiceRpc, topologyToSdkEntityConverter);
        allDataRequirements.add(containerResizeSpecFactory.getContainerResizeSpec());

        // Create a spec for host provision
        allDataRequirements.add(new DataRequirementSpecBuilder()
                .addMatchCriteria(actionInfo -> actionInfo.hasProvision())
                .addMatchCriteria(actionInfo -> EntityType.PHYSICAL_MACHINE.getNumber() ==
                        actionInfo.getProvision().getEntityToClone().getType())
                .addDataRequirement("clusterDisplayName", actionInfo ->
                        getClusterNameForAction(actionInfo))
                .build());

        // Create a spec for storage provision
        StorageProvisionSpecFactory storageProvisionSpecFactory =
                new StorageProvisionSpecFactory(searchServiceRpc);
        allDataRequirements.add(storageProvisionSpecFactory.getStorageProvisionSpec());

        // Create a spec for volume scale
        VolumeScaleSpecFactory volumeScaleSpecFactory
                = new VolumeScaleSpecFactory(searchServiceRpc);
        allDataRequirements.add(volumeScaleSpecFactory.getVolumeScaleSpec());
    }

    /**
     * Retrieves additional context data used for action execution
     * Data will be retrieved only for special cases that match the provided actionInfo
     *
     * @param actionInfo action information, used to determine which special cases to apply
     * @return a list of additional context data used for action execution
     */
    @Nonnull
    public List<ContextData> getContextData(@Nonnull final ActionInfo actionInfo) {
        return allDataRequirements.stream()
                .filter(dataRequirementSpec -> dataRequirementSpec.matchesAllCriteria(actionInfo))
                .map(dataRequirementSpec -> dataRequirementSpec.retrieveRequiredData(actionInfo))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    // TODO: Look this up from the Group service. There is no current way to get this in XL.
    private String getClusterNameForAction(ActionInfo actionInfo) {
        return "";
    }

}
